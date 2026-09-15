#ifndef OBSERVABILITY_METRICS_REGISTRY_H
#define OBSERVABILITY_METRICS_REGISTRY_H

#include <array>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

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

private:
    MetricsRegistry();

    std::string labels(const std::map<std::string, std::string>& values) const;
    std::string escape_label_value(const std::string& value) const;

    mutable std::mutex mutex_;
    std::map<std::string, uint64_t> http_requests_;
    std::array<uint64_t, 13> http_latency_buckets_;
    uint64_t http_latency_count_;
    double http_latency_sum_;
    std::map<std::string, uint64_t> cache_results_;
    uint64_t kafka_publish_success_;
    uint64_t kafka_publish_failure_;
    uint64_t kafka_enqueue_accepted_;
    uint64_t kafka_enqueue_dropped_;
    uint64_t kafka_produce_accepted_;
    uint64_t kafka_produce_failed_;
    uint64_t kafka_delivered_;
    uint64_t kafka_delivery_failed_;
    uint64_t log_enqueue_accepted_;
    uint64_t log_enqueue_dropped_;
    std::map<std::string, uint64_t> origin_cap_acquired_;
    std::map<std::string, uint64_t> origin_cap_rejected_;
};

#endif
