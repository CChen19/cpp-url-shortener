#include "../CGImysql/sql_connection_pool.h"
#include "../lock/locker.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <pthread.h>
#include <unistd.h>

namespace {

int g_failures = 0;

void expect_true(bool cond, const char* msg) {
    if (!cond) {
        std::fprintf(stderr, "FAIL: %s\n", msg);
        ++g_failures;
    } else {
        std::printf("ok: %s\n", msg);
    }
}

struct TimedWaitArg {
    sem* s;
    int timeout_ms;
    std::atomic<bool>* done;
    std::atomic<bool>* ok;
};

void* timed_wait_worker(void* arg) {
    TimedWaitArg* a = static_cast<TimedWaitArg*>(arg);
    a->ok->store(a->s->wait(a->timeout_ms));
    a->done->store(true);
    return nullptr;
}

void test_sem_timedwait_timeout() {
    sem s(0);
    auto start = std::chrono::steady_clock::now();
    bool ok = s.wait(40);
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    expect_true(!ok, "sem timedwait returns false on timeout");
    expect_true(elapsed >= 35, "sem timedwait waited roughly the timeout");
}

void test_sem_timedwait_success() {
    sem s(0);
    std::atomic<bool> done(false);
    std::atomic<bool> ok(false);
    TimedWaitArg arg = {&s, 500, &done, &ok};
    pthread_t tid;
    pthread_create(&tid, nullptr, timed_wait_worker, &arg);
    usleep(20 * 1000);
    s.post();
    pthread_join(tid, nullptr);
    expect_true(done.load() && ok.load(), "sem timedwait succeeds when posted");
}

void test_pool_acquire_timeout_without_mysql() {
    connection_pool* pool = connection_pool::GetInstance();
    pool->init_for_test(1, 50);

    expect_true(pool->GetFreeConn() == 1, "test pool starts with one free conn");

    MYSQL* first = pool->GetConnection();
    expect_true(first != nullptr, "first acquire succeeds");
    expect_true(pool->GetFreeConn() == 0, "no free conn while held");

    auto start = std::chrono::steady_clock::now();
    MYSQL* second = pool->GetConnection();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();
    expect_true(second == nullptr, "second acquire times out when pool exhausted");
    expect_true(elapsed >= 40, "exhausted acquire waited for timeout");

    expect_true(pool->ReleaseConnection(first), "release succeeds");
    expect_true(pool->GetFreeConn() == 1, "free conn restored after release");

    MYSQL* third = pool->GetConnection(100);
    expect_true(third != nullptr, "acquire after release succeeds");
    pool->ReleaseConnection(third);

    pool->DestroyPool();
    expect_true(pool->GetConnection(10) == nullptr, "destroyed pool returns null");
}

} // namespace

int main() {
    test_sem_timedwait_timeout();
    test_sem_timedwait_success();
    test_pool_acquire_timeout_without_mysql();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all tests passed\n");
    return 0;
}
