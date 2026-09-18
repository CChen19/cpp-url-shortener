// Regression test: log rotation fopen failure must not crash the process.
//
// Fix under test (log/log.cpp, log/log.h): on rotation the old FILE* is
// fclosed and set to NULL first; if fopen(new_log) fails the error is
// reported and m_fp stays NULL; every fputs path (sync write_log fallback and
// async_write_log) NULL-checks m_fp, and the next write_log retries the open
// via the `m_fp == NULL ||` condition.
//
// Pre-fix behaviour: fopen failure left m_fp == NULL and the unconditional
// fputs(log_str, m_fp) dereferenced it -> SIGSEGV. This test therefore dies
// mid-run on the old code and never reaches the later assertions.
//
// The Log singleton is process-wide, so this file has exactly one main and
// must not be combined with other Log::init users.
#include "../log/log.h"
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
#include <cstdio>
#include <cstring>
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

std::string make_temp_dir()
{
    char tmpl[] = "/tmp/tws_log_nullfp_XXXXXX";
    char *dir = mkdtemp(tmpl);
    if (dir == NULL)
    {
        std::perror("mkdtemp");
        std::exit(2);
    }
    return std::string(dir);
}

int count_log_files(const std::string &dir)
{
    int n = 0;
    DIR *d = opendir(dir.c_str());
    if (d == NULL)
    {
        return -1;
    }
    while (dirent *e = readdir(d))
    {
        if (std::strstr(e->d_name, "ServerLog") != NULL)
        {
            ++n;
        }
    }
    closedir(d);
    return n;
}

void remove_tree(const std::string &dir)
{
    DIR *d = opendir(dir.c_str());
    if (d != NULL)
    {
        while (dirent *e = readdir(d))
        {
            if (std::strcmp(e->d_name, ".") == 0 ||
                std::strcmp(e->d_name, "..") == 0)
            {
                continue;
            }
            unlink((dir + "/" + e->d_name).c_str());
        }
        closedir(d);
    }
    rmdir(dir.c_str());
}

} // namespace

int main()
{
    const std::string dir = make_temp_dir();
    const std::string log_path = dir + "/ServerLog";

    // Sync mode (max_queue_size = 0), rotate every 4 lines.
    Log *log = Log::get_instance();
    expect_true(log->init(log_path.c_str(), 1 /*close_log*/, 4096, 4, 0),
                "rotation: Log::init to temp dir (sync, split_lines=4)");
    expect_true(count_log_files(dir) == 1, "rotation: initial log file exists");

    // 3 lines: below the split threshold, no rotation yet.
    for (int i = 1; i <= 3; ++i)
    {
        log->write_log(1, "warmup line %d", i);
    }
    log->flush();

    // Make rotation fopen fail: drop the write bit (uid != 0 on CI/dev boxes).
    // If writes still succeed (e.g. running as root), fall back to renaming
    // the directory away so fopen gets ENOENT either way.
    bool dir_hidden = false;
    if (chmod(dir.c_str(), 0555) != 0)
    {
        std::fprintf(stderr, "note: chmod failed (%s)\n", std::strerror(errno));
    }
    {
        FILE *probe = fopen((dir + "/.probe").c_str(), "a");
        if (probe != NULL)
        {
            fclose(probe);
            unlink((dir + "/.probe").c_str());
            chmod(dir.c_str(), 0700);
            const std::string hidden = dir + ".hidden";
            if (rename(dir.c_str(), hidden.c_str()) == 0)
            {
                dir_hidden = true;
            }
        }
    }

    // Lines 4..13: line 4 triggers the first rotation -> fclose + failing
    // fopen (m_fp = NULL); every later line re-enters the rotation branch via
    // the m_fp == NULL retry and must skip fputs without crashing.
    // Pre-fix this SEGVs on the first fputs(NULL) right here.
    for (int i = 4; i <= 13; ++i)
    {
        log->write_log(1, "post-rotation-failure line %d", i);
    }
    log->flush(); // must not crash with m_fp == NULL either
    std::printf("ok: rotation: writes + flush survived fopen failure\n");

    // Restore and prove the retry path reopens the log: the next write enters
    // the rotation branch (m_fp still NULL) and now fopen succeeds into a NEW
    // segment file that really receives bytes.
    chmod(dir.c_str(), 0700);
    if (dir_hidden)
    {
        const std::string hidden = dir + ".hidden";
        rename(hidden.c_str(), dir.c_str());
    }
    log->write_log(1, "recovery line 14");
    log->flush();

    const int files = count_log_files(dir);
    expect_true(files == 2, "rotation: reopen produced a second segment file");

    if (files == 2)
    {
        // Some segment file must contain the recovery line.
        bool recovered = false;
        DIR *d = opendir(dir.c_str());
        while (dirent *e = readdir(d))
        {
            std::string name = e->d_name;
            if (name.find("ServerLog") == std::string::npos)
            {
                continue;
            }
            FILE *f = fopen((dir + "/" + name).c_str(), "r");
            if (f == NULL)
            {
                continue;
            }
            char buf[512];
            while (fgets(buf, sizeof(buf), f) != NULL)
            {
                if (std::strstr(buf, "recovery line 14") != NULL)
                {
                    recovered = true;
                    break;
                }
            }
            fclose(f);
            if (recovered)
            {
                break;
            }
        }
        closedir(d);
        expect_true(recovered, "rotation: recovery line landed after reopen");
    }

    remove_tree(dir);

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all log rotation null-fp tests passed\n");
    return 0;
}
