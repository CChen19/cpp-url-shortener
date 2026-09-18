#ifndef HTTPCONNECTION_H
#define HTTPCONNECTION_H
#include <unistd.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/uio.h>
#include <atomic>
#include <cstdint>

#include "../lock/locker.h"
#include "../CGImysql/sql_connection_pool.h"
#include "../timer/lst_timer.h"
#include "../log/log.h"
#include "request.h"
#include "response.h"

class http_conn
{
public:
    // Large enough for create: headers + JSON wrapping a 2KB long_url.
    static const int READ_BUFFER_SIZE = 8192;
    static const int WRITE_BUFFER_SIZE = 8192;

    enum METHOD
    {
        GET = 0,
        POST,
        PUT,
        DELETE,
        HEAD,
        OPTIONS
    };
    enum CHECK_STATE
    {
        CHECK_STATE_REQUESTLINE = 0,
        CHECK_STATE_HEADER,
        CHECK_STATE_CONTENT
    };
    enum HTTP_CODE
    {
        NO_REQUEST,
        GET_REQUEST,
        BAD_REQUEST,
        REQUEST_ENTITY_TOO_LARGE,
        INTERNAL_ERROR,
        CLOSED_CONNECTION
    };
    enum LINE_STATUS
    {
        LINE_OK = 0,
        LINE_BAD,
        LINE_OPEN
    };

public:
    http_conn() : m_generation(0), m_pending_close(false) {}
    ~http_conn() {}

public:
    void init(int sockfd, const sockaddr_in &addr, int TRIGMode, int close_log);
    // Invalidate the slot so in-flight worker results for an old generation are ignored.
    // Does not close the fd; the event loop (cb_func / deal_timer) is the only closer.
    void invalidate();
    uint64_t current_generation() const;
    bool generation_matches(uint64_t expected) const;
    // Worker entry: applies EPOLL arming only if expected still matches.
    void process(uint64_t expected_generation);
    bool read_once();
    bool write();
    // True when request bytes were already read past the parser cursor
    // (pipelined / same-segment request waiting to be dispatched).
    bool has_pipelined_input() const { return m_read_idx > m_checked_idx; }
    // Best-effort 503 for queue-full on the event-loop thread; caller closes fd.
    void reject_overload();
    sockaddr_in *get_address()
    {
        return &m_address;
    }

public:
    static int m_epollfd;
    static int m_user_count;
    MYSQL *mysql;
    int m_state;

private:
    void init();
    // Keep any already-received request bytes across the parser reset so a
    // pipelined request is not silently dropped by init()'s buffer clear.
    void retain_pipelined_bytes();
    HTTP_CODE process_read();
    bool process_write(const HttpResponse &resp);
    HTTP_CODE parse_request_line(char *text);
    HTTP_CODE parse_headers(char *text);
    HTTP_CODE parse_content(char *text);
    char *get_line() { return m_read_buf + m_start_line; };
    LINE_STATUS parse_line();

private:
    int m_sockfd;
    sockaddr_in m_address;
    char m_read_buf[READ_BUFFER_SIZE];
    long m_read_idx;
    long m_checked_idx;
    int m_start_line;
    char m_write_buf[WRITE_BUFFER_SIZE];
    int m_write_idx;
    CHECK_STATE m_check_state;
    METHOD m_method;

    HttpRequest m_request;
    char *m_url;
    char *m_version;
    char *m_host;
    long m_content_length;
    bool m_linger;

    struct iovec m_iv[2];
    int m_iv_count;
    int bytes_to_send;
    int bytes_have_send;

    int m_TRIGMode;
    int m_close_log;

    // Bumped on accept/init and on timeout/close; enqueue captures it for workers.
    std::atomic<uint64_t> m_generation;
    // Set by a worker when the response cannot be buffered; event-loop write() closes.
    bool m_pending_close;

    // Arm EPOLLIN/EPOLLOUT only while the connection generation still matches.
    bool arm_epoll_if_current(uint64_t expected_generation, int ev);
};

#endif
