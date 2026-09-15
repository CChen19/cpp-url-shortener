#include "http_conn.h"
#include "protocol_utils.h"
#include "router.h"
#include "../observability/metrics_registry.h"
#include "../observability/structured_logger.h"
#include <nlohmann/json.hpp>
#include <chrono>

int setnonblocking(int fd)
{
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
    return old_option;
}

void addfd(int epollfd, int fd, bool one_shot, int TRIGMode)
{
    epoll_event event;
    event.data.fd = fd;

    if (1 == TRIGMode)
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    else
        event.events = EPOLLIN | EPOLLRDHUP;

    if (one_shot)
        event.events |= EPOLLONESHOT;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);
}

void removefd(int epollfd, int fd)
{
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

void modfd(int epollfd, int fd, int ev, int TRIGMode)
{
    epoll_event event;
    event.data.fd = fd;

    if (1 == TRIGMode)
        event.events = ev | EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
    else
        event.events = ev | EPOLLONESHOT | EPOLLRDHUP;

    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}

int http_conn::m_user_count = 0;
int http_conn::m_epollfd = -1;

uint64_t http_conn::current_generation() const
{
    return m_generation.load(std::memory_order_acquire);
}

bool http_conn::generation_matches(uint64_t expected) const
{
    return current_generation() == expected;
}

void http_conn::invalidate()
{
    // Bump first so any in-flight worker fails generation_matches before fd reuse.
    m_generation.fetch_add(1, std::memory_order_acq_rel);
    m_pending_close = false;
    m_sockfd = -1;
}

bool http_conn::arm_epoll_if_current(uint64_t expected_generation, int ev)
{
    if (!generation_matches(expected_generation) || m_sockfd < 0)
    {
        return false;
    }
    modfd(m_epollfd, m_sockfd, ev, m_TRIGMode);
    return generation_matches(expected_generation);
}

void http_conn::reject_overload()
{
    if (m_sockfd < 0)
    {
        return;
    }
    static const char kBody[] = "{\"error\":\"service unavailable\"}";
    char response[256];
    int n = snprintf(response, sizeof(response),
                     "HTTP/1.1 503 Service Unavailable\r\n"
                     "Content-Type: application/json\r\n"
                     "Connection: close\r\n"
                     "Content-Length: %zu\r\n"
                     "\r\n"
                     "%s",
                     sizeof(kBody) - 1, kBody);
    if (n > 0)
    {
        send(m_sockfd, response, static_cast<size_t>(n), MSG_NOSIGNAL);
    }
}

void http_conn::init(int sockfd, const sockaddr_in &addr, int TRIGMode, int close_log)
{
    m_sockfd = sockfd;
    m_address = addr;
    m_TRIGMode = TRIGMode;
    m_close_log = close_log;

    addfd(m_epollfd, sockfd, true, m_TRIGMode);
    m_user_count++;

    // New connection identity for this fd/slot; stale worker results must not apply.
    m_generation.fetch_add(1, std::memory_order_acq_rel);
    init();
}

void http_conn::init()
{
    mysql = NULL;
    bytes_to_send = 0;
    bytes_have_send = 0;
    m_check_state = CHECK_STATE_REQUESTLINE;
    // HTTP/1.1 default is keep-alive unless Connection: close.
    m_linger = true;
    m_method = GET;
    m_url = 0;
    m_version = 0;
    m_content_length = 0;
    m_host = 0;
    m_start_line = 0;
    m_checked_idx = 0;
    m_read_idx = 0;
    m_write_idx = 0;
    m_state = 0;
    m_pending_close = false;
    m_request = HttpRequest{};
    memset(m_read_buf, '\0', READ_BUFFER_SIZE);
    memset(m_write_buf, '\0', WRITE_BUFFER_SIZE);
}

http_conn::LINE_STATUS http_conn::parse_line()
{
    char temp;
    for (; m_checked_idx < m_read_idx; ++m_checked_idx)
    {
        temp = m_read_buf[m_checked_idx];
        if (temp == '\r')
        {
            if ((m_checked_idx + 1) == m_read_idx)
                return LINE_OPEN;
            else if (m_read_buf[m_checked_idx + 1] == '\n')
            {
                m_read_buf[m_checked_idx++] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LINE_OK;
            }
            return LINE_BAD;
        }
        else if (temp == '\n')
        {
            if (m_checked_idx > 1 && m_read_buf[m_checked_idx - 1] == '\r')
            {
                m_read_buf[m_checked_idx - 1] = '\0';
                m_read_buf[m_checked_idx++] = '\0';
                return LINE_OK;
            }
            return LINE_BAD;
        }
    }
    return LINE_OPEN;
}

bool http_conn::read_once()
{
    // Buffer full: still return true so process_read can emit 413 (no silent truncate).
    if (m_read_idx >= READ_BUFFER_SIZE)
    {
        return true;
    }
    int bytes_read = 0;

    if (0 == m_TRIGMode)
    {
        bytes_read = recv(m_sockfd, m_read_buf + m_read_idx, READ_BUFFER_SIZE - m_read_idx, 0);
        if (bytes_read <= 0)
        {
            return false;
        }
        m_read_idx += bytes_read;
        return true;
    }
    else
    {
        bool got_data = false;
        while (m_read_idx < READ_BUFFER_SIZE)
        {
            bytes_read = recv(m_sockfd, m_read_buf + m_read_idx, READ_BUFFER_SIZE - m_read_idx, 0);
            if (bytes_read == -1)
            {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                return false;
            }
            else if (bytes_read == 0)
            {
                return got_data;
            }
            m_read_idx += bytes_read;
            got_data = true;
        }
        return got_data || m_read_idx >= READ_BUFFER_SIZE;
    }
}

http_conn::HTTP_CODE http_conn::parse_request_line(char *text)
{
    m_url = strpbrk(text, " \t");
    if (!m_url)
    {
        return BAD_REQUEST;
    }
    *m_url++ = '\0';
    char *method = text;
    if (strcasecmp(method, "GET") == 0)
        m_method = GET;
    else if (strcasecmp(method, "POST") == 0)
        m_method = POST;
    else if (strcasecmp(method, "PUT") == 0)
        m_method = PUT;
    else if (strcasecmp(method, "DELETE") == 0)
        m_method = DELETE;
    else
        return BAD_REQUEST;

    m_url += strspn(m_url, " \t");
    m_version = strpbrk(m_url, " \t");
    if (!m_version)
        return BAD_REQUEST;
    *m_version++ = '\0';
    m_version += strspn(m_version, " \t");
    if (strcasecmp(m_version, "HTTP/1.1") != 0)
        return BAD_REQUEST;
    if (strncasecmp(m_url, "http://", 7) == 0)
    {
        m_url += 7;
        m_url = strchr(m_url, '/');
    }

    if (strncasecmp(m_url, "https://", 8) == 0)
    {
        m_url += 8;
        m_url = strchr(m_url, '/');
    }

    if (!m_url || m_url[0] != '/')
        return BAD_REQUEST;

    static const char *method_names[] = {"GET", "POST", "PUT", "DELETE", "HEAD", "OPTIONS"};
    m_request.method = method_names[m_method];

    char *query = strchr(m_url, '?');
    if (query) {
        *query++ = '\0';
        m_request.query = query;
    }
    m_request.path = m_url;

    m_check_state = CHECK_STATE_HEADER;
    return NO_REQUEST;
}

http_conn::HTTP_CODE http_conn::parse_headers(char *text)
{
    if (text[0] == '\0')
    {
        if (m_content_length != 0)
        {
            // Body must fit in the remaining read buffer (plus room to NUL-terminate).
            if (m_content_length < 0 ||
                m_checked_idx + m_content_length >= READ_BUFFER_SIZE)
            {
                return REQUEST_ENTITY_TOO_LARGE;
            }
            m_check_state = CHECK_STATE_CONTENT;
            return NO_REQUEST;
        }
        return GET_REQUEST;
    }
    else if (strncasecmp(text, "Connection:", 11) == 0)
    {
        text += 11;
        text += strspn(text, " \t");
        if (strcasecmp(text, "keep-alive") == 0)
        {
            m_linger = true;
        }
        else if (strcasecmp(text, "close") == 0)
        {
            m_linger = false;
        }
        m_request.headers["Connection"] = text;
    }
    else if (strncasecmp(text, "Content-length:", 15) == 0)
    {
        text += 15;
        text += strspn(text, " \t");
        long length = 0;
        if (!parse_content_length(text, &length))
        {
            return BAD_REQUEST;
        }
        // Absolute cap: body alone cannot exceed the read buffer.
        if (length >= READ_BUFFER_SIZE)
        {
            return REQUEST_ENTITY_TOO_LARGE;
        }
        m_content_length = length;
        m_request.headers["Content-Length"] = text;
    }
    else if (is_unsupported_body_encoding_header(text))
    {
        // Unimplemented body encodings make request boundaries ambiguous.
        return BAD_REQUEST;
    }
    else if (strncasecmp(text, "Host:", 5) == 0)
    {
        text += 5;
        text += strspn(text, " \t");
        m_host = text;
        m_request.headers["Host"] = text;
    }
    else
    {
        char *colon = strchr(text, ':');
        if (colon) {
            std::string key(text, colon - text);
            char *val = colon + 1;
            val += strspn(val, " \t");
            m_request.headers[key] = val;
        }
    }
    return NO_REQUEST;
}

http_conn::HTTP_CODE http_conn::parse_content(char *text)
{
    if (m_read_idx >= (m_content_length + m_checked_idx))
    {
        text[m_content_length] = '\0';
        m_request.body = std::string(text, m_content_length);
        return GET_REQUEST;
    }
    return NO_REQUEST;
}

http_conn::HTTP_CODE http_conn::process_read()
{
    LINE_STATUS line_status = LINE_OK;
    HTTP_CODE ret = NO_REQUEST;
    char *text = 0;

    while ((m_check_state == CHECK_STATE_CONTENT && line_status == LINE_OK) || ((line_status = parse_line()) == LINE_OK))
    {
        text = get_line();
        m_start_line = m_checked_idx;
        LOG_INFO("%s", text);
        switch (m_check_state)
        {
        case CHECK_STATE_REQUESTLINE:
        {
            ret = parse_request_line(text);
            if (ret == BAD_REQUEST)
                return BAD_REQUEST;
            break;
        }
        case CHECK_STATE_HEADER:
        {
            ret = parse_headers(text);
            if (ret == BAD_REQUEST)
                return BAD_REQUEST;
            if (ret == REQUEST_ENTITY_TOO_LARGE)
                return REQUEST_ENTITY_TOO_LARGE;
            else if (ret == GET_REQUEST)
            {
                return GET_REQUEST;
            }
            break;
        }
        case CHECK_STATE_CONTENT:
        {
            ret = parse_content(text);
            if (ret == GET_REQUEST)
                return GET_REQUEST;
            line_status = LINE_OPEN;
            break;
        }
        default:
            return INTERNAL_ERROR;
        }
    }
    if (line_status == LINE_BAD)
    {
        return BAD_REQUEST;
    }
    // Incomplete request that already filled the buffer: reject, do not truncate.
    if (m_read_idx >= READ_BUFFER_SIZE)
    {
        return REQUEST_ENTITY_TOO_LARGE;
    }
    return NO_REQUEST;
}

bool http_conn::process_write(const HttpResponse &resp)
{
    std::string raw = resp.serialize();
    if (static_cast<int>(raw.size()) >= WRITE_BUFFER_SIZE)
        return false;

    memcpy(m_write_buf, raw.c_str(), raw.size());
    m_write_idx = raw.size();

    m_iv[0].iov_base = m_write_buf;
    m_iv[0].iov_len = m_write_idx;
    m_iv_count = 1;
    bytes_to_send = m_write_idx;
    return true;
}

bool http_conn::write()
{
    int temp = 0;

    if (m_pending_close)
    {
        m_pending_close = false;
        return false;
    }

    if (bytes_to_send == 0)
    {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_TRIGMode);
        init();
        return true;
    }

    while (1)
    {
        temp = writev(m_sockfd, m_iv, m_iv_count);

        if (temp < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                modfd(m_epollfd, m_sockfd, EPOLLOUT, m_TRIGMode);
                return true;
            }
            return false;
        }

        bytes_have_send += temp;
        bytes_to_send -= temp;
        m_iv[0].iov_base = m_write_buf + bytes_have_send;
        m_iv[0].iov_len = bytes_to_send;

        if (bytes_to_send <= 0)
        {
            modfd(m_epollfd, m_sockfd, EPOLLIN, m_TRIGMode);

            if (m_linger)
            {
                init();
                return true;
            }
            else
            {
                return false;
            }
        }
    }
}

void http_conn::process(uint64_t expected_generation)
{
    if (!generation_matches(expected_generation))
    {
        return;
    }

    const auto started_at = std::chrono::steady_clock::now();
    HTTP_CODE read_ret = process_read();
    if (read_ret == NO_REQUEST)
    {
        arm_epoll_if_current(expected_generation, EPOLLIN);
        return;
    }

    HttpResponse resp;

    if (read_ret == BAD_REQUEST)
    {
        resp.set_status(400);
        resp.set_json({{"error", "bad request"}});
    }
    else if (read_ret == REQUEST_ENTITY_TOO_LARGE)
    {
        resp.set_status(413);
        resp.set_json({{"error", "request entity too large"}});
        m_linger = false;
    }
    else if (read_ret == INTERNAL_ERROR)
    {
        resp.set_status(500);
        resp.set_json({{"error", "internal server error"}});
    }
    else
    {
        m_request.mysql = nullptr;
        if (!Router::instance().dispatch(m_request, resp))
        {
            m_request.route_pattern = "unmatched";
            resp.set_status(404);
            resp.set_json({{"error", "not found"}, {"path", m_request.path}});
        }
    }

    resp.set_header("Connection", m_linger ? "keep-alive" : "close");

    const auto finished_at = std::chrono::steady_clock::now();
    const double duration_seconds = std::chrono::duration<double>(
        finished_at - started_at).count();
    const std::string route = m_request.route_pattern.empty()
        ? "parse_error"
        : m_request.route_pattern;
    MetricsRegistry::instance().observe_http_request(
        m_request.method.empty() ? "UNKNOWN" : m_request.method,
        route,
        resp.status_code(),
        duration_seconds);
    StructuredLogger::instance().log_access(
        m_request,
        resp.status_code(),
        duration_seconds * 1000.0,
        inet_ntoa(m_address.sin_addr));

    if (!generation_matches(expected_generation))
    {
        return;
    }

    if (!process_write(resp))
    {
        // Do not close from the worker; ask the event loop to close via write().
        if (generation_matches(expected_generation) && m_sockfd >= 0)
        {
            m_pending_close = true;
            arm_epoll_if_current(expected_generation, EPOLLOUT);
        }
        return;
    }
    arm_epoll_if_current(expected_generation, EPOLLOUT);
}
