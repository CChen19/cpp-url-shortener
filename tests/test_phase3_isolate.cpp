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

void test_redis_probe_keeps_client_and_can_restore() {
    MetricsRegistry::instance().reset_for_test();
#ifdef HAVE_REDIS_PLUS_PLUS
    // Boot against a closed port: client constructs, ping fails. Must KEEP the
    // client so probe_loop can PING later. Old code redis_.reset() fails here.
    Config cfg;
    cfg.redis_enabled = true;
    cfg.redis_uri = "tcp://127.0.0.1:1";
    cfg.redis_connect_timeout_ms = 50;
    cfg.redis_socket_timeout_ms = 50;
    cfg.redis_pool_size = 1;
    cfg.redis_probe_interval_ms = 100;

    ShortUrlCache& cache = ShortUrlCache::instance();
    cache.init(cfg);
    // Stop the background probe so this test owns the restore path.
    cache.shutdown();

    expect_true(!cache.redis_available(),
                "init ping failure leaves redis_available false");
    expect_true(cache.has_redis_client_for_test(),
                "init ping failure keeps redis_ for probe (not reset to null)");
    expect_true(cache.probe_restore_if_client_present_for_test(),
                "probe path restores only when a client is held");
    expect_true(cache.redis_available(),
                "probe restore flips redis_available when client present");

    // Null-client gate: clearing the handle must prevent restore (documents
    // why reset-after-ping-fail was broken).
    cache.mark_redis_unavailable_for_test();
    // Re-init with redis disabled clears the client.
    Config off;
    off.redis_enabled = false;
    cache.init(off);
    cache.shutdown();
    expect_true(!cache.has_redis_client_for_test(),
                "disabled redis has no client");
    expect_true(!cache.probe_restore_if_client_present_for_test(),
                "probe cannot restore when redis_ is null");
    expect_true(!cache.redis_available(),
                "null client leaves redis unavailable");
#else
    std::printf("skip: redis probe keep-client test (no redis++)\n");
#endif
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
    test_redis_probe_keeps_client_and_can_restore();
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
