// Parser / URL-safety unit tests for phase 1c protocol hardening.
#include "../http/protocol_utils.h"
#include <cstdio>
#include <string>

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

void test_content_length_ok()
{
    long n = -1;
    expect_true(parse_content_length("0", &n) && n == 0, "CL 0");
    expect_true(parse_content_length("2048", &n) && n == 2048, "CL 2048");
    expect_true(parse_content_length("8191", &n) && n == 8191, "CL 8191");
}

void test_content_length_negative()
{
    long n = 0;
    expect_true(!parse_content_length("-1", &n), "CL negative");
    expect_true(!parse_content_length("-0", &n), "CL -0");
}

void test_content_length_overflow()
{
    long n = 0;
    expect_true(!parse_content_length("999999999999999999999999", &n),
                "CL overflow digits");
    expect_true(!parse_content_length("9223372036854775808", &n),
                "CL > LONG_MAX");
    expect_true(!parse_content_length("", &n), "CL empty");
    expect_true(!parse_content_length("12a", &n), "CL trailing junk");
    expect_true(!parse_content_length(" 12", &n), "CL leading space");
}

void test_transfer_encoding_rejected()
{
    expect_true(is_unsupported_body_encoding_header("Transfer-Encoding: chunked"),
                "TE chunked");
    expect_true(is_unsupported_body_encoding_header("transfer-encoding: gzip"),
                "TE case");
    expect_true(is_unsupported_body_encoding_header("Content-Encoding: gzip"),
                "CE gzip");
    expect_true(!is_unsupported_body_encoding_header("Content-Length: 10"),
                "CL not encoding");
}

void test_url_crlf_rejected()
{
    expect_true(is_http_url_safe_for_location("https://example.com/a"),
                "safe url");
    expect_true(!is_http_url_safe_for_location(
                    "https://evil.com/\r\nSet-Cookie: x=1"),
                "CRLF in url");
    expect_true(!is_http_url_safe_for_location("https://evil.com/\nX"),
                "LF in url");
    expect_true(!is_http_url_safe_for_location("https://evil.com/\x01path"),
                "CTL in url");
    expect_true(!is_http_url_safe_for_location("ftp://example.com/a"),
                "non-http scheme");
}

void test_public_base_join()
{
    expect_true(join_public_base_url("http://short.example", "abc") ==
                    "http://short.example/abc",
                "base join");
    expect_true(join_public_base_url("http://short.example/", "abc") ==
                    "http://short.example/abc",
                "base trailing slash");
    expect_true(join_public_base_url("", "abc") == "/abc", "empty base");
}

} // namespace

int main()
{
    test_content_length_ok();
    test_content_length_negative();
    test_content_length_overflow();
    test_transfer_encoding_rejected();
    test_url_crlf_rejected();
    test_public_base_join();

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all http protocol tests passed\n");
    return 0;
}
