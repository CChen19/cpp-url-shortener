// Harness: enqueue carries (conn, generation); stale generations must not apply.
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <pthread.h>
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

// Mirrors http_conn generation + threadpool append_p / run() protocol.
struct ConnSlot
{
    std::atomic<uint64_t> generation;
    std::atomic<int> apply_count;
    std::atomic<int> stale_skips;

    ConnSlot() : generation(0), apply_count(0), stale_skips(0) {}

    uint64_t bump_on_accept()
    {
        return generation.fetch_add(1, std::memory_order_acq_rel) + 1;
    }

    void invalidate_on_close()
    {
        generation.fetch_add(1, std::memory_order_acq_rel);
    }

    bool generation_matches(uint64_t expected) const
    {
        return generation.load(std::memory_order_acquire) == expected;
    }

    bool try_apply(uint64_t enqueued_gen)
    {
        if (!generation_matches(enqueued_gen))
        {
            stale_skips.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        apply_count.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
};

struct WorkerArg
{
    ConnSlot *slot;
    uint64_t enqueued_gen;
    int delay_us;
};

void *stale_worker(void *arg)
{
    WorkerArg *a = static_cast<WorkerArg *>(arg);
    if (a->delay_us > 0)
    {
        usleep(static_cast<useconds_t>(a->delay_us));
    }
    a->slot->try_apply(a->enqueued_gen);
    return nullptr;
}

void test_stale_enqueue_ignored()
{
    ConnSlot slot;
    const uint64_t gen1 = slot.bump_on_accept();
    expect_true(gen1 == 1, "first accept yields generation 1");

    slot.invalidate_on_close();
    expect_true(!slot.try_apply(gen1), "stale generation after close is ignored");
    expect_true(slot.stale_skips.load() == 1, "stale skip counted");
    expect_true(slot.apply_count.load() == 0, "stale result not applied");

    const uint64_t gen2 = slot.bump_on_accept();
    expect_true(slot.try_apply(gen2), "current generation is applied");
    expect_true(!slot.try_apply(gen1), "older generation still ignored after reuse");
    expect_true(slot.apply_count.load() == 1, "only one successful apply");
}

void test_concurrent_stale_worker()
{
    ConnSlot slot;
    const uint64_t gen = slot.bump_on_accept();

    WorkerArg arg = {&slot, gen, 30 * 1000};
    pthread_t tid;
    pthread_create(&tid, nullptr, stale_worker, &arg);

    usleep(5 * 1000);
    slot.invalidate_on_close();
    const uint64_t gen_new = slot.bump_on_accept();

    pthread_join(tid, nullptr);

    expect_true(slot.stale_skips.load() == 1, "delayed worker skipped stale generation");
    expect_true(slot.apply_count.load() == 0, "delayed worker did not apply after reuse");
    expect_true(slot.try_apply(gen_new), "new generation still works");
    expect_true(slot.apply_count.load() == 1, "new generation applied once");
}

} // namespace

int main()
{
    test_stale_enqueue_ignored();
    test_concurrent_stale_worker();

    if (g_failures != 0)
    {
        std::fprintf(stderr, "%d test(s) failed\n", g_failures);
        return 1;
    }
    std::printf("all tests passed\n");
    return 0;
}
