// Regression tests for pipelined request handling (P1 fix 2026-xx).
//
// Fix under test:
//   1. parse_content() consumes the body (m_checked_idx += m_content_length)
//      so bytes after a POST body parse as the next request.
//   2. write() on a keep-alive connection calls retain_pipelined_bytes()
//      instead of init(), so request bytes already read past the parser
//      cursor survive the parser reset.
//   3. webserver dealwithwrite() re-dispatches the worker when
//      has_pipelined_input() — this test simulates that by calling
//      process()+write() again from the test thread.
//
// Harness: socketpair + the real http_conn. The event loop owns recv/send
// (read_once / write); the worker logic (process) runs synchronously in this
// thread, exactly like tests/test_http_parse_e2e.cpp.
//
// Pre-fix behaviour (init() cleared the whole read buffer): the piggybacked
// second request is silently dropped, has_pipelined_input() stays false after
// the first write() and only ONE response ever arrives -> every test here
// fails (case 1/2) or the client would hang until timeout in production.
#include "../http/http_conn.h"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>

namespace {

int g_failures = 0;

void expect_true(bool cond, const char *msg)
{
    if (!cond)
    {
        std::fprintf(stderr, "FAIL: %s\n", msg);
        ++g_failures;
    }
    else
    {
        std::printf("ok: %s\n", msg);
    }
}

// Minimal stand-in for the event loop + worker pair.
class ConnHarness
{
public:
    ConnHarness()
    {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds_) != 0)
        {
            std::perror("socketpair");
            std::exit(2);
        }
        sockaddr_in addr;
        std::memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        // TRIGMode 0 (LT), close_log 1: no log file, modfd on m_epollfd is
        // harmless because the test owns a real epoll instance.
        conn_.init(fds_[0], addr, 0, 1);
        gen_ = conn_.current_generation();
    }
    ~ConnHarness()
    {
        close(fds_[0]);
        close(fds_[1]);
    }

    void client_send(const std::string &bytes)
    {
        ssize_t ignored = send(fds_[1], bytes.data(), bytes.size(), 0);
        (void)ignored;
    }

    bool server_read_once() { return conn_.read_once(); }
    void worker_process() { conn_.process(gen_); }
    bool server_write() { return conn_.write(); }
    bool has_pipelined_input() const { return conn_.has_pipelined_input(); }

    // Client side drains whatever the server wrote so far (non-blocking).
    std::string drain()
    {
        std::string out;
        char buf[8192];
        ssize_t n;
        while ((n = recv(fds_[1], buf, sizeof(buf), MSG_DONTWAIT)) > 0)
        {
            out.append(buf, static_cast<size_t>(n));
        }
        return out;
    }

private:
    int fds_[2];
    http_conn conn_;
    uint64_t gen_;
};

int count_occurrences(const std::string &hay, const char *needle)
{
    int n = 0;
    for (size_t pos = 0; (pos = hay.find(needle, pos)) != std::string::npos;
         pos += 1)
    {
        ++n;
    }
    return n;
}

static const char *kStatusLine = "HTTP/1.1 404";

// Two complete GET requests arriving in one segment. No routes registered,
// so each well-formed request yields one 404.
void test_two_gets_in_one_packet()
{
    ConnHarness h;
    h.client_send("GET /first HTTP/1.1\r\nHost: h\r\n\r\n"
                  "GET /second HTTP/1.1\r\nHost: h\r\n\r\n");

    expect_true(h.server_read_once(), "pipelining: read_once #1");
    h.worker_process();
    expect_true(h.server_write(), "pipelining: write #1");

    // KEY pre-fix discriminator: the piggybacked request must still be in the
    // parser buffer after the keep-alive reset.
    expect_true(h.has_pipelined_input(),
                "pipelining: second request retained after write #1");

    h.worker_process();
    expect_true(h.server_write(), "pipelining: write #2");
    expect_true(!h.has_pipelined_input(),
                "pipelining: buffer drained after second dispatch");

    const std::string all = h.drain();
    expect_true(count_occurrences(all, kStatusLine) == 2,
                "pipelining: exactly two responses came back");
    const size_t first = all.find("\"path\":\"/first\"");
    const size_t second = all.find("\"path\":\"/second\"");
    expect_true(first != std::string::npos && second != std::string::npos &&
                    first < second,
                "pipelining: /first answered before /second");
}

// POST with a body plus a GET piggybacked after the body: the body must be
// consumed exactly, so the next request parses from the byte after it.
void test_post_body_then_piggybacked_get()
{
    ConnHarness h;
    h.client_send("POST /submit HTTP/1.1\r\nHost: h\r\n"
                  "Content-Length: 5\r\n\r\n"
                  "hello"
                  "GET /after HTTP/1.1\r\nHost: h\r\n\r\n");

    expect_true(h.server_read_once(), "post+get: read_once #1");
    h.worker_process();
    expect_true(h.server_write(), "post+get: write #1");
    expect_true(h.has_pipelined_input(),
                "post+get: GET after body retained");

    h.worker_process();
    expect_true(h.server_write(), "post+get: write #2");
    expect_true(!h.has_pipelined_input(), "post+get: buffer drained");

    const std::string all = h.drain();
    expect_true(count_occurrences(all, kStatusLine) == 2,
                "post+get: two responses (pre-fix: 1, body ate the GET)");
    const size_t submit = all.find("\"path\":\"/submit\"");
    const size_t after = all.find("\"path\":\"/after\"");
    expect_true(submit != std::string::npos, "post+get: POST parsed");
    expect_true(after != std::string::npos,
                "post+get: body consumed exactly, GET parsed from after it");
    expect_true(all.find("\"path\":\"/hello") == std::string::npos &&
                    count_occurrences(all, "400") == 0,
                "post+get: no body leakage into the next request line");
}

// The second request only partially arrived: no response for it, no crash;
// once the rest arrives it completes normally.
// Note: the event loop never calls write() here — write() is only entered
// when a worker armed EPOLLOUT after process_write(), which does not happen
// for NO_REQUEST. So the harness must not call write() after the second
// process() either.
void test_partial_second_request()
{
    ConnHarness h;
    h.client_send("GET /full HTTP/1.1\r\nHost: h\r\n\r\n"
                  "GET /hal");

    expect_true(h.server_read_once(), "partial: read_once #1");
    h.worker_process();
    expect_true(h.server_write(), "partial: write #1");

    const std::string after_first = h.drain();
    expect_true(count_occurrences(after_first, kStatusLine) == 1,
                "partial: exactly one response after the full request");

    h.worker_process(); // incomplete request -> NO_REQUEST, no response

    const std::string after_half = h.drain();
    expect_true(after_half.empty(),
                "partial: NO_REQUEST produced no bytes for the client");

    h.client_send("f HTTP/1.1\r\nHost: h\r\n\r\n");
    expect_true(h.server_read_once(), "partial: read_once #2 (remainder)");
    h.worker_process();
    expect_true(h.server_write(), "partial: write #2");

    const std::string all = after_first + after_half + h.drain();
    expect_true(count_occurrences(all, kStatusLine) == 2,
                "partial: second request completed after remainder");
    expect_true(all.find("\"path\":\"/half\"") != std::string::npos,
                "partial: reassembled request line parsed as /half");
    expect_true(!h.has_pipelined_input(), "partial: buffer drained at end");
}

// Behavior note (RFC framing semantics): body bytes beyond Content-Length
// are no longer part of THIS request's body; the fix retains them and they
// parse as the next request. Harness registers no routes (same as
// test_http_parse_e2e), so any well-formed request yields 404 — that stands
// in for the "2xx" a routed handler would return.
void test_overlong_body_leftover_becomes_garbage_request()
{
    ConnHarness h;
    // CL=2 declares "ab"; "cdefgh" belongs to the next framing unit. No CRLF
    // yet: the garbage is buffered but an incomplete line must NOT produce a
    // response.
    h.client_send("POST /submit HTTP/1.1\r\nHost: h\r\n"
                  "Content-Length: 2\r\n\r\n"
                  "abcdefgh");

    expect_true(h.server_read_once(), "overlong: read_once #1");
    h.worker_process();
    expect_true(h.server_write(), "overlong: write #1");
    expect_true(h.has_pipelined_input(), "overlong: leftover buffered");

    const std::string first = h.drain(); // collect response #1
    h.worker_process(); // incomplete line -> NO_REQUEST, no response
    expect_true(h.drain().empty(),
                "overlong: incomplete leftover produced no response");

    // Complete the garbage request line; it parses -> 400.
    h.client_send("\r\n");
    expect_true(h.server_read_once(), "overlong: read_once #2 (CRLF)");
    h.worker_process();
    expect_true(h.server_write(), "overlong: write #2");

    const std::string all = first + h.drain();
    // 404 for the POST + 400 for the garbage line.
    expect_true(count_occurrences(all, "HTTP/1.1 ") == 2,
                "overlong: two responses (pre-fix: leftover dropped, only 1)");
    // First response: well-formed POST accepted by the parser (404 = no
    // routes in this harness). Second response: 400 garbage request line.
    const size_t submit = all.find("\"path\":\"/submit\"");
    expect_true(submit != std::string::npos,
                "overlong: first request accepted (404 no-routes harness)");
    const size_t garbage = submit == std::string::npos
        ? std::string::npos
        : all.find("HTTP/1.1 400", submit);
    expect_true(garbage != std::string::npos,
                "overlong: leftover 'cdefgh' parsed as next request -> 400");
    expect_true(!h.has_pipelined_input(), "overlong: buffer drained");
}

// Same framing semantics, but the leftover is a VALID piggybacked GET: it
// must parse cleanly from the byte after the declared body — the old code
// NUL-terminated the body in place, clobbering the 'G' and turning the next
// request into a 400.
void test_exact_body_then_valid_piggybacked_get()
{
    ConnHarness h;
    h.client_send("POST /submit HTTP/1.1\r\nHost: h\r\n"
                  "Content-Length: 2\r\n\r\n"
                  "ab"
                  "GET /after HTTP/1.1\r\nHost: h\r\n\r\n");

    expect_true(h.server_read_once(), "cl2+get: read_once #1");
    h.worker_process();
    expect_true(h.server_write(), "cl2+get: write #1");
    expect_true(h.has_pipelined_input(), "cl2+get: GET retained after body");

    h.worker_process();
    expect_true(h.server_write(), "cl2+get: write #2");
    expect_true(!h.has_pipelined_input(), "cl2+get: buffer drained");

    const std::string all = h.drain();
    expect_true(count_occurrences(all, kStatusLine) == 2,
                "cl2+get: two responses");
    expect_true(all.find("HTTP/1.1 400") == std::string::npos,
                "cl2+get: piggybacked GET not mangled into a 400");
    const size_t after = all.find("\"path\":\"/after\"");
    expect_true(after != std::string::npos,
                "cl2+get: body-end terminator not clobbering the 'G'; "
                "next request parsed as /after");
}

} // namespace

int main()
{
    // http_conn::init()/modfd register the test sockets here; harmless.
    http_conn::m_epollfd = epoll_create1(0);
    if (http_conn::m_epollfd < 0)
    {
        std::perror("epoll_create1");
        return 2;
    }

    test_two_gets_in_one_packet();
    test_post_body_then_piggybacked_get();
    test_partial_second_request();
    test_overlong_body_leftover_becomes_garbage_request();
    test_exact_body_then_valid_piggybacked_get();

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all http pipelining tests passed\n");
    return 0;
}
