#include "../analytics/click_event_producer.h"
#include "../config/config.h"
#include "../http/request.h"
#include "../observability/metrics_registry.h"
#include "../observability/structured_logger.h"
#include "../shorturl/short_url_cache.h"
#include <cstdio>
#include <string>

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

void test_log_queue_drop() {
    MetricsRegistry::instance().reset_for_test();
    // No worker: lines stay queued so overflow is deterministic.
    StructuredLogger::instance().init_for_test(2, "/dev/null", false);

    HttpRequest req;
    req.method = "GET";
    req.path = "/abc";
    req.route_pattern = "/{code}";

    StructuredLogger::instance().log_access(req, 302, 0.1, "127.0.0.1");
    StructuredLogger::instance().log_access(req, 302, 0.1, "127.0.0.1");
    StructuredLogger::instance().log_access(req, 302, 0.1, "127.0.0.1");

    expect_true(MetricsRegistry::instance().log_enqueue_dropped() >= 1,
                "full structured-log queue increments drop counter");

    StructuredLogger::instance().shutdown(100);
}

void test_kafka_queue_drop() {
    MetricsRegistry::instance().reset_for_test();
    // Enqueue-only (no worker) so capacity fills and overflows.
    ClickEventProducer::instance().init_for_test(2, /*null_sink=*/true,
                                                 /*start_worker=*/false);

    HttpRequest req;
    req.method = "GET";
    req.path = "/abc";
    req.headers["User-Agent"] = "test-agent";

    expect_true(ClickEventProducer::instance().publish_click(req, "c1"),
                "first click enqueues");
    expect_true(ClickEventProducer::instance().publish_click(req, "c2"),
                "second click enqueues");
    expect_true(!ClickEventProducer::instance().publish_click(req, "c3"),
                "third click drops when queue full");
    expect_true(MetricsRegistry::instance().kafka_enqueue_dropped() >= 1,
                "full analytics queue increments drop counter");

    ClickEventProducer::instance().shutdown(100);
}

void test_redis_probe_recovery_flag() {
    MetricsRegistry::instance().reset_for_test();
    ShortUrlCache& cache = ShortUrlCache::instance();

    cache.mark_redis_unavailable_for_test();
    expect_true(!cache.redis_available(),
                "redis unavailable flag can be cleared for outage");

    cache.simulate_probe_success_for_test();
    expect_true(cache.redis_available(),
                "simulated probe success restores redis_available");
}

void test_origin_cap_when_redis_down() {
    MetricsRegistry::instance().reset_for_test();
    ShortUrlCache& cache = ShortUrlCache::instance();
    cache.configure_origin_caps_for_test(/*redirect*/1, /*create*/1);
    cache.mark_redis_unavailable_for_test();

    OriginBudgetGuard first =
        cache.try_acquire_origin(ShortUrlCache::OriginKind::Redirect);
    expect_true(first.held(), "first redirect origin slot acquired");

    OriginBudgetGuard second =
        cache.try_acquire_origin(ShortUrlCache::OriginKind::Redirect);
    expect_true(!second.held(),
                "origin cap rejects extra redirect DB when Redis is down");
    expect_true(MetricsRegistry::instance().origin_cap_rejected("redirect") >= 1,
                "rejected origin cap is counted");

    // Create budget is independent of redirect.
    OriginBudgetGuard create =
        cache.try_acquire_origin(ShortUrlCache::OriginKind::Create);
    expect_true(create.held(),
                "create budget separate from redirect when Redis is down");

    OriginBudgetGuard create2 =
        cache.try_acquire_origin(ShortUrlCache::OriginKind::Create);
    expect_true(!create2.held(), "create origin cap rejects excess");
}

} // namespace

int main() {
    test_log_queue_drop();
    test_kafka_queue_drop();
    test_redis_probe_recovery_flag();
    test_origin_cap_when_redis_down();

    // Leave singletons in a clean stopped state for process exit.
    ClickEventProducer::instance().shutdown(100);
    StructuredLogger::instance().shutdown(100);
    ShortUrlCache::instance().shutdown();

    if (g_failures > 0) {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all phase3 isolate tests passed\n");
    return 0;
}
