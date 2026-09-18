// Regression tests for the request-line parser (remote-crash fixes 2026-09-17).
// Drives the real http_conn::process() over a socketpair so the exact code
// path that used to SEGV is exercised end to end.
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

// Runs one raw request through the real parse -> route -> serialize path.
// No routes are registered, so any well-formed request yields 404.
std::string run_request(const std::string &raw)
{
    int fds[2];
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0)
    {
        std::perror("socketpair");
        std::exit(2);
    }

    sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;

    http_conn conn;
    conn.init(fds[0], addr, 0 /*TRIGMode LT*/, 1 /*close_log*/);
    const uint64_t gen = conn.current_generation();

    ssize_t ignored = send(fds[1], raw.data(), raw.size(), 0);
    (void)ignored;

    // The event loop owns recv (main-thread I/O); the worker only processes.
    if (!conn.read_once())
    {
        std::fprintf(stderr, "FAIL: read_once\n");
        std::exit(2);
    }
    conn.process(gen);

    std::string response;
    if (conn.write())
    {
        char buf[8192];
        ssize_t n;
        while ((n = recv(fds[1], buf, sizeof(buf), MSG_DONTWAIT)) > 0)
        {
            response.append(buf, static_cast<size_t>(n));
        }
    }

    close(fds[0]);
    close(fds[1]);
    return response;
}

// Pre-fix: strchr returned NULL and the next strncasecmp dereferenced it.
void test_absolute_uri_without_path_is_bad_request()
{
    const std::string resp = run_request(
        "GET http://noslash.example HTTP/1.1\r\nHost: h\r\n\r\n");
    expect_true(resp.rfind("HTTP/1.1 400", 0) == 0,
                "absolute URI without path -> 400, not a crash");
}

// Absolute-form with a path must keep parsing (not regress the fix).
void test_absolute_uri_with_path_still_parses()
{
    const std::string resp = run_request(
        "GET http://example.com/abc HTTP/1.1\r\nHost: h\r\n\r\n");
    expect_true(resp.rfind("HTTP/1.1 404", 0) == 0,
                "absolute URI with path -> parsed, 404 (no routes)");
}

void test_plain_path_still_parses()
{
    const std::string resp = run_request(
        "GET /abc HTTP/1.1\r\nHost: h\r\n\r\n");
    expect_true(resp.rfind("HTTP/1.1 404", 0) == 0,
                "plain path -> parsed, 404 (no routes)");
}

void test_unknown_method_is_bad_request()
{
    const std::string resp = run_request(
        "BREW /abc HTTP/1.1\r\nHost: h\r\n\r\n");
    expect_true(resp.rfind("HTTP/1.1 400", 0) == 0,
                "unknown method -> 400");
}

} // namespace

int main()
{
    http_conn::m_epollfd = epoll_create1(0);
    if (http_conn::m_epollfd < 0)
    {
        std::perror("epoll_create1");
        return 2;
    }

    test_absolute_uri_without_path_is_bad_request();
    test_absolute_uri_with_path_still_parses();
    test_plain_path_still_parses();
    test_unknown_method_is_bad_request();

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all http parse e2e tests passed\n");
    return 0;
}
