#ifndef OBSERVABILITY_METRICS_REGISTRY_H
#define OBSERVABILITY_METRICS_REGISTRY_H

#include <array>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

// Request-path observe_* uses per-shard mutexes (thread-id hashed).
// /metrics scrape and getters lock shards only to aggregate.
class MetricsRegistry {
public:
    static MetricsRegistry& instance();

    void observe_http_request(const std::string& method,
                              const std::string& route,
                              int status,
                              double duration_seconds);
    void observe_cache_result(const std::string& result);

    // Legacy: enqueue accepted vs dropped/unavailable on the request path.
    void observe_kafka_publish(bool success);

    void observe_kafka_enqueue(bool accepted);
    void observe_kafka_produce_accepted(bool ok);
    void observe_kafka_delivery(bool success);
    void observe_log_enqueue(bool accepted);

    void observe_origin_cap(const std::string& kind, bool acquired);

    uint64_t kafka_enqueue_dropped() const;
    uint64_t kafka_produce_accepted() const;
    uint64_t kafka_delivered() const;
    uint64_t log_enqueue_dropped() const;
    uint64_t origin_cap_rejected(const std::string& kind) const;

    // Test helper: reset counters (not for production request path).
    void reset_for_test();

    std::string render_prometheus();

    // Exposed for tests: shard count used by observe path.
    static constexpr size_t shard_count() { return kShardCount; }

private:
    static constexpr size_t kShardCount = 16;

    struct Shard {
        mutable std::mutex mutex;
        std::map<std::string, uint64_t> http_requests;
        std::array<uint64_t, 13> http_latency_buckets{};
        uint64_t http_latency_count = 0;
        double http_latency_sum = 0.0;
        std::map<std::string, uint64_t> cache_results;
        uint64_t kafka_publish_success = 0;
        uint64_t kafka_publish_failure = 0;
        uint64_t kafka_enqueue_accepted = 0;
        uint64_t kafka_enqueue_dropped = 0;
        uint64_t kafka_produce_accepted = 0;
        uint64_t kafka_produce_failed = 0;
        uint64_t kafka_delivered = 0;
        uint64_t kafka_delivery_failed = 0;
        uint64_t log_enqueue_accepted = 0;
        uint64_t log_enqueue_dropped = 0;
        std::map<std::string, uint64_t> origin_cap_acquired;
        std::map<std::string, uint64_t> origin_cap_rejected;
    };

    MetricsRegistry();

    Shard& shard_for_this_thread();

    std::string labels(const std::map<std::string, std::string>& values) const;
    std::string escape_label_value(const std::string& value) const;

    std::array<Shard, kShardCount> shards_;
};

#endif
