// Regression tests for the snowflake worker-id bits (P1 fix).
//
// Fix under test (shorturl/snowflake.h/.cpp): id layout changed from
// (seconds << 12) | seq to (seconds << 22) | (worker << 12) | seq with
// kWorkerBits = 10; set_worker_id(long) pins the worker id (negative values
// are ignored, values are masked to 0..1023); the default is pid-derived.
// Config snowflake_worker_id >= 0 feeds set_worker_id from main().
//
// Regression-revert check is not applicable here: the old 22-bit-free layout
// has no observable "old behaviour" for worker bits — these tests pin the NEW
// layout so it cannot silently change again.
#include "../shorturl/snowflake.h"
#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <set>
#include <vector>

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

uint64_t worker_bits_of(uint64_t id)
{
    return (id >> 12) & 1023ULL; // kWorkerBits = 10, kSequenceBits = 12
}

} // namespace

int main()
{
    SnowflakeIdGenerator gen;

    // 1. set_worker_id lands in bits 12..21.
    SnowflakeIdGenerator::set_worker_id(123);
    const uint64_t pinned = gen.next_id();
    expect_true(worker_bits_of(pinned) == 123,
                "snowflake: worker bits (id>>12)&1023 == 123");
    expect_true((pinned >> 22) > 0, "snowflake: timestamp bits still present");

    // 2. 10000 ids from one instance: unique and strictly increasing.
    std::vector<uint64_t> ids;
    ids.reserve(10000);
    for (int i = 0; i < 10000; ++i)
    {
        ids.push_back(gen.next_id()); // mutex-serialized inside
    }
    std::set<uint64_t> unique(ids.begin(), ids.end());
    expect_true(unique.size() == ids.size(),
                "snowflake: 10000 ids, no duplicates");
    bool strictly_increasing = true;
    for (size_t i = 1; i < ids.size(); ++i)
    {
        if (ids[i] <= ids[i - 1])
        {
            strictly_increasing = false;
            break;
        }
    }
    expect_true(strictly_increasing, "snowflake: ids strictly increasing");

    // 3. set_worker_id(-1) must NOT change the currently pinned id.
    SnowflakeIdGenerator::set_worker_id(-1);
    const uint64_t after_negative = gen.next_id();
    expect_true(worker_bits_of(after_negative) == 123,
                "snowflake: set_worker_id(-1) keeps current value");

    // 4. Values are masked into 0..1023 (1029 & 1023 == 5).
    SnowflakeIdGenerator::set_worker_id(1029);
    const uint64_t clamped = gen.next_id();
    expect_true(worker_bits_of(clamped) == 5,
                "snowflake: out-of-range worker id clamped (& 1023)");

    // 5. Different worker bits -> different ids.
    SnowflakeIdGenerator other;
    SnowflakeIdGenerator::set_worker_id(1);
    const uint64_t w1 = other.next_id();
    SnowflakeIdGenerator::set_worker_id(2);
    const uint64_t w2 = other.next_id();
    expect_true(w1 != w2, "snowflake: worker 1 vs 2 ids differ");
    expect_true(worker_bits_of(w1) == 1 && worker_bits_of(w2) == 2,
                "snowflake: worker bits distinguish the two generators");
    expect_true((w1 >> 22) == (w2 >> 22) || (w1 >> 22) == (w2 >> 22) - 1,
                "snowflake: worker bits sit below an unchanged timestamp");

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all snowflake worker tests passed\n");
    return 0;
}
