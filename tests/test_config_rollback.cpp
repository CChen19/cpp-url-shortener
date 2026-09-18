// Regression test: Config::load() failure must leave the object untouched.
//
// Fix under test (config/config.cpp): load() snapshots `const Config rollback
// = *this;` before parsing; the YAML::Exception handler restores `*this =
// rollback` and returns false.
//
// Pre-fix behaviour: fields parsed before the malformed entry were already
// overwritten, so the process kept running on a half-default / half-file mix
// while claiming "using defaults". On the old code the first two assertions
// after load(bad) fail (port == 1234, thread_num == 3 leaked in).
#include "../config/config.h"
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
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
    char tmpl[] = "/tmp/tws_cfg_rollback_XXXXXX";
    char *dir = mkdtemp(tmpl);
    if (dir == NULL)
    {
        std::perror("mkdtemp");
        std::exit(2);
    }
    return std::string(dir);
}

void write_file(const std::string &path, const char *content)
{
    FILE *f = fopen(path.c_str(), "w");
    if (f == NULL)
    {
        std::perror("fopen config");
        std::exit(2);
    }
    fputs(content, f);
    fclose(f);
}

// Valid prefix, then a type error deep in the file: everything before the
// error would stick on the pre-fix code.
const char *kBadYaml =
    "server:\n"
    "  port: 1234\n"
    "  thread_num: 3\n"
    "log:\n"
    "  path: /tmp/leaked.log\n"
    "sharding:\n"
    "  table_count: \"four\"\n";

const char *kGoodYaml =
    "server:\n"
    "  port: 1234\n"
    "  thread_num: 3\n"
    "sharding:\n"
    "  table_count: 7\n"
    "snowflake:\n"
    "  worker_id: 5\n";

} // namespace

int main()
{
    const std::string dir = make_temp_dir();
    const std::string bad_path = dir + "/bad.yaml";
    const std::string good_path = dir + "/good.yaml";
    write_file(bad_path, kBadYaml);
    write_file(good_path, kGoodYaml);

    // --- Bad file: load fails, object must roll back to defaults. ---
    Config c; // defaults: port 9006, thread_num 8, shard_table_count 4
    expect_true(c.port == 9006, "rollback: default port before load");

    expect_true(c.load(bad_path) == false,
                "rollback: load(bad yaml) returns false");
    expect_true(c.port == 9006,
                "rollback: port still default after failed load");
    expect_true(c.thread_num == 8,
                "rollback: thread_num not leaked from the valid prefix");
    expect_true(c.log_path == "./ServerLog",
                "rollback: log_path not leaked from the valid prefix");
    expect_true(c.shard_table_count == 4,
                "rollback: shard_table_count still default");

    // --- Good file: load succeeds and every field applies. ---
    Config g;
    expect_true(g.load(good_path) == true, "rollback: load(good yaml) true");
    expect_true(g.port == 1234, "rollback: good port applied");
    expect_true(g.thread_num == 3, "rollback: good thread_num applied");
    expect_true(g.shard_table_count == 7, "rollback: good table_count applied");
    expect_true(g.snowflake_worker_id == 5, "rollback: worker id applied");

    unlink(bad_path.c_str());
    unlink(good_path.c_str());
    rmdir(dir.c_str());

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all config rollback tests passed\n");
    return 0;
}
