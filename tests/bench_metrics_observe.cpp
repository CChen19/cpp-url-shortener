// In-process evidence for MetricsRegistry observe contention.
// Not a cluster QPS claim — wall time for concurrent observe_* only.
#include "../observability/metrics_registry.h"
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

namespace {

double run_observe_burst(int threads, int iters_per_thread) {
    MetricsRegistry::instance().reset_for_test();
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));
    const auto start = std::chrono::steady_clock::now();
    for (int t = 0; t < threads; ++t) {
        workers.emplace_back([iters_per_thread, t]() {
            const std::string method = "GET";
            const std::string route = "/{code}";
            for (int i = 0; i < iters_per_thread; ++i) {
                // L1 302 path: cache observe + HTTP observe (typical).
                MetricsRegistry::instance().observe_cache_result("local_hit");
                MetricsRegistry::instance().observe_http_request(
                    method, route, 302, 0.0001);
                if ((i & 7) == 0) {
                    MetricsRegistry::instance().observe_log_enqueue(true);
                }
            }
            (void)t;
        });
    }
    for (auto& w : workers) {
        w.join();
    }
    const auto end = std::chrono::steady_clock::now();
    return std::chrono::duration<double>(end - start).count();
}

} // namespace

int main() {
    const int threads = 8;
    const int iters = 50000;
    // Warmup
    (void)run_observe_burst(threads, 1000);
    const double secs = run_observe_burst(threads, iters);
    const double ops = static_cast<double>(threads) * static_cast<double>(iters);
    std::printf("threads=%d iters_per_thread=%d wall_s=%.6f observes≈%.0f "
                "(cache+http per iter; not server QPS)\n",
                threads, iters, secs, ops);
    std::printf("wall_ns_per_iter=%.1f\n", (secs * 1e9) / ops);

    // Correctness smoke: scrape must see aggregated HTTP counts.
    const std::string prom = MetricsRegistry::instance().render_prometheus();
    if (prom.find("shorturl_http_requests_total") == std::string::npos) {
        std::fprintf(stderr, "FAIL: scrape missing http counter\n");
        return 1;
    }
    std::printf("scrape_ok bytes=%zu\n", prom.size());
    return 0;
}
