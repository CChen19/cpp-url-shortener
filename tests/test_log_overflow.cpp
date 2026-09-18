// Regression for the Log::write_log heap overflow (remote-crash fix 2026-09-17).
// This target is built with -fsanitize=address, so the pre-fix out-of-bounds
// store at m_buf[n + m] aborts the test instead of silently corrupting the heap.
#include "../log/log.h"
#include <sys/stat.h>
#include <cstdio>
#include <ctime>
#include <fstream>
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

// Mirrors Log::init naming for "./testlogs/server": dir + %Y_%m_%d_ + name.
std::string expected_log_path()
{
    std::time_t t = std::time(nullptr);
    std::tm tm_buf;
    localtime_r(&t, &tm_buf);
    char tail[32];
    std::strftime(tail, sizeof(tail), "%Y_%m_%d_", &tm_buf);
    return std::string("./testlogs/") + tail + "server";
}

size_t file_size_or_zero(const std::string &path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0 ? static_cast<size_t>(st.st_size) : 0;
}

// Reads the bytes appended since `before`, waiting up to ~2s for the async
// writer thread to drain the queue.
std::string appended_region(Log *log, const std::string &path, size_t before)
{
    std::string region;
    for (int i = 0; i < 40; ++i)
    {
        usleep(50 * 1000);
        // The async thread fputs into the stdio buffer; flush so the poll
        // below can observe what has been written so far.
        log->flush();
        std::ifstream in(path.c_str(), std::ios::binary);
        if (!in)
        {
            continue;
        }
        in.seekg(static_cast<std::streamoff>(before));
        std::string all((std::istreambuf_iterator<char>(in)),
                        std::istreambuf_iterator<char>());
        if (!all.empty() && all[all.size() - 1] == '\n')
        {
            region = all;
            break;
        }
    }
    return region;
}

} // namespace

int main()
{
    mkdir("./testlogs", 0755);

    Log *log = Log::get_instance();
    expect_true(log->init("./testlogs/server", 0 /*close_log*/,
                          2000 /*buf*/, 800000 /*split*/, 800 /*queue*/),
                "log init");

    const std::string path = expected_log_path();
    const size_t before = file_size_or_zero(path);

    // 4000-char payload: pre-fix wrote ~2000 bytes past the 2000-byte m_buf.
    std::string long_line(4000, 'A');
    log->write_log(1, "%s", long_line.c_str());
    log->flush();

    const std::string region = appended_region(log, path, before);
    expect_true(!region.empty(), "truncated line appended");
    expect_true(region.size() <= 2100,
                "line clamped to the log buffer, not the full 4000 chars");
    expect_true(region.find(std::string(1965, 'A')) == std::string::npos,
                "payload truncated (no run of 1965+ 'A')");

    // A normal line must still come through intact after the clamp.
    const size_t before2 = before + region.size();
    log->write_log(1, "%s", "hello regression");
    log->flush();

    const std::string region2 = appended_region(log, path, before2);
    expect_true(region2.find("hello regression") != std::string::npos,
                "short line intact after overflow clamp");

    // The legacy async log thread never exits; skip static destruction and
    // stdio exit-flushing (a closed FILE* under a live thread is the exact
    // race this test must not depend on).
    std::fflush(stdout);
    std::fflush(stderr);
    _Exit(g_failures == 0 ? 0 : 1);
}
