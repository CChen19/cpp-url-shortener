#ifndef HTTP_PROTOCOL_UTILS_H
#define HTTP_PROTOCOL_UTILS_H

#include <climits>
#include <cstddef>
#include <cstring>
#include <string>

// Shared HTTP framing / Location-safety helpers (header-only for C++14 tests).

inline bool parse_content_length(const char *text, long *out)
{
    if (!text || !out || !*text)
    {
        return false;
    }
    if (*text == '-')
    {
        return false;
    }
    if (*text == '+')
    {
        ++text;
        if (!*text)
        {
            return false;
        }
    }

    unsigned long long value = 0;
    bool saw_digit = false;
    for (; *text; ++text)
    {
        if (*text < '0' || *text > '9')
        {
            return false;
        }
        saw_digit = true;
        unsigned digit = static_cast<unsigned>(*text - '0');
        if (value > (ULLONG_MAX - digit) / 10ULL)
        {
            return false;
        }
        value = value * 10ULL + digit;
    }
    if (!saw_digit || value > static_cast<unsigned long long>(LONG_MAX))
    {
        return false;
    }
    *out = static_cast<long>(value);
    return true;
}

// Reject CR/LF and other CTL bytes that break Location / header framing.
inline bool contains_ctl_or_separator(const std::string &value)
{
    for (size_t i = 0; i < value.size(); ++i)
    {
        unsigned char c = static_cast<unsigned char>(value[i]);
        if (c < 0x20 || c == 0x7f)
        {
            return true;
        }
    }
    return false;
}

inline bool is_http_url_safe_for_location(const std::string &url)
{
    if (url.size() < 8 || url.size() > 2048)
    {
        return false;
    }
    if (contains_ctl_or_separator(url))
    {
        return false;
    }
    return (url.compare(0, 7, "http://") == 0) ||
           (url.compare(0, 8, "https://") == 0);
}

inline bool is_unsupported_body_encoding_header(const char *text)
{
    if (!text)
    {
        return false;
    }
    return strncasecmp(text, "Transfer-Encoding:", 18) == 0 ||
           strncasecmp(text, "Content-Encoding:", 17) == 0;
}

inline std::string join_public_base_url(const std::string &base, const std::string &code)
{
    if (base.empty())
    {
        return "/" + code;
    }
    std::string out = base;
    while (!out.empty() && out[out.size() - 1] == '/')
    {
        out.resize(out.size() - 1);
    }
    return out + "/" + code;
}

#endif
