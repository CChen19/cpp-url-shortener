#include "../observability/metrics_registry.h"
#include <atomic>
#include <cstdio>
#include <regex>
#include <string>
#include <thread>
#include <vector>

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

void test_concurrent_observe_preserves_counts() {
    MetricsRegistry::instance().reset_for_test();
    const int threads = 8;
    const int iters = 25000;
    std::vector<std::thread> workers;
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([iters]() {
            for (int i = 0; i < iters; ++i) {
                MetricsRegistry::instance().observe_cache_result("local_hit");
                MetricsRegistry::instance().observe_http_request(
                    "GET", "/{code}", 302, 0.0001);
                MetricsRegistry::instance().observe_log_enqueue(true);
                MetricsRegistry::instance().observe_kafka_enqueue(true);
            }
        });
    }
    for (auto& w : workers) {
        w.join();
    }

    const uint64_t expected =
        static_cast<uint64_t>(threads) * static_cast<uint64_t>(iters);
    const std::string prom = MetricsRegistry::instance().render_prometheus();

    expect_true(MetricsRegistry::shard_count() == 16, "16 observe shards");
    expect_true(MetricsRegistry::instance().log_enqueue_dropped() == 0,
                "no log drops in success-only observe");
    expect_true(MetricsRegistry::instance().kafka_enqueue_dropped() == 0,
                "no kafka drops in success-only observe");

    // scrape aggregates across shards
    expect_true(prom.find("shorturl_http_requests_total{") != std::string::npos,
                "scrape emits http counter");
    expect_true(prom.find("method=\"GET\"") != std::string::npos,
                "scrape has GET label");
    expect_true(prom.find("route=\"/{code}\"") != std::string::npos,
                "scrape has route label");
    expect_true(prom.find("status_class=\"3xx\"") != std::string::npos,
                "scrape has 3xx status class");

    // Parse histogram count line
    std::regex count_re(
        R"(shorturl_http_request_duration_seconds_count ([0-9]+))");
    std::smatch cm;
    expect_true(std::regex_search(prom, cm, count_re), "histogram count present");
    if (!cm.empty()) {
        expect_true(std::stoull(cm[1].str()) == expected,
                    "http latency count equals concurrent observes");
    }

    std::regex cache_re(
        R"(shorturl_cache_requests_total\{result="local_hit"\} ([0-9]+))");
    std::smatch cache_m;
    expect_true(std::regex_search(prom, cache_m, cache_re),
                "cache local_hit present");
    if (!cache_m.empty()) {
        expect_true(std::stoull(cache_m[1].str()) == expected,
                    "cache local_hit equals concurrent observes");
    }

    std::regex log_re(
        R"(shorturl_log_enqueue_total\{result="accepted"\} ([0-9]+))");
    std::smatch log_m;
    expect_true(std::regex_search(prom, log_m, log_re), "log accepted present");
    if (!log_m.empty()) {
        expect_true(std::stoull(log_m[1].str()) == expected,
                    "log accepted equals concurrent observes");
    }

    std::regex http_re(
        R"(shorturl_http_requests_total\{method="GET",route="/\{code\}",status_class="3xx"\} ([0-9]+))");
    std::smatch http_m;
    expect_true(std::regex_search(prom, http_m, http_re),
                "http GET /{code} 3xx present");
    if (!http_m.empty()) {
        expect_true(std::stoull(http_m[1].str()) == expected,
                    "http counter equals concurrent observes");
    }

}

void test_reset_clears_all_shards() {
    MetricsRegistry::instance().reset_for_test();
    std::atomic<int> started{0};
    std::vector<std::thread> workers;
    for (int t = 0; t < 8; ++t) {
        workers.emplace_back([&started]() {
            started.fetch_add(1);
            MetricsRegistry::instance().observe_cache_result("local_miss");
            MetricsRegistry::instance().observe_origin_cap("redirect", false);
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    expect_true(started.load() == 8, "all workers ran");
    expect_true(MetricsRegistry::instance().origin_cap_rejected("redirect") == 8,
                "origin rejected summed across shards");

    MetricsRegistry::instance().reset_for_test();
    expect_true(MetricsRegistry::instance().origin_cap_rejected("redirect") == 0,
                "reset_for_test clears all shards");
    const std::string prom = MetricsRegistry::instance().render_prometheus();
    expect_true(prom.find("local_miss") == std::string::npos,
                "reset clears cache series from scrape");
}

} // namespace

int main() {
    test_concurrent_observe_preserves_counts();
    test_reset_clears_all_shards();
    if (g_failures != 0) {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all tests passed\n");
    return 0;
}
