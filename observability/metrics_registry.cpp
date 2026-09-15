#include "metrics_registry.h"
#include <functional>
#include <iomanip>
#include <sstream>
#include <thread>

namespace {

const double kLatencyBuckets[] = {
    0.001, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250,
    0.500, 1.000, 2.500, 5.000, 10.000
};

std::string status_class(int status) {
    if (status >= 100 && status < 600) {
        return std::to_string(status / 100) + "xx";
    }
    return "unknown";
}

void add_map(std::map<std::string, uint64_t>& dst,
             const std::map<std::string, uint64_t>& src) {
    for (const auto& item : src) {
        dst[item.first] += item.second;
    }
}

} // namespace

MetricsRegistry& MetricsRegistry::instance() {
    static MetricsRegistry registry;
    return registry;
}

MetricsRegistry::MetricsRegistry() = default;

MetricsRegistry::Shard& MetricsRegistry::shard_for_this_thread() {
    const size_t idx =
        std::hash<std::thread::id>{}(std::this_thread::get_id()) % kShardCount;
    return shards_[idx];
}

void MetricsRegistry::observe_http_request(const std::string& method,
                                           const std::string& route,
                                           int status,
                                           double duration_seconds) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    const std::string key = method + "|" + route + "|" + status_class(status);
    shard.http_requests[key]++;
    for (size_t i = 0; i < sizeof(kLatencyBuckets) / sizeof(kLatencyBuckets[0]); ++i) {
        if (duration_seconds <= kLatencyBuckets[i]) {
            shard.http_latency_buckets[i]++;
        }
    }
    shard.http_latency_buckets[shard.http_latency_buckets.size() - 1]++;
    shard.http_latency_count++;
    shard.http_latency_sum += duration_seconds;
}

void MetricsRegistry::observe_cache_result(const std::string& result) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    shard.cache_results[result]++;
}

void MetricsRegistry::observe_kafka_publish(bool success) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (success) {
        shard.kafka_publish_success++;
    } else {
        shard.kafka_publish_failure++;
    }
}

void MetricsRegistry::observe_kafka_enqueue(bool accepted) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (accepted) {
        shard.kafka_enqueue_accepted++;
        shard.kafka_publish_success++;
    } else {
        shard.kafka_enqueue_dropped++;
        shard.kafka_publish_failure++;
    }
}

void MetricsRegistry::observe_kafka_produce_accepted(bool ok) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (ok) {
        shard.kafka_produce_accepted++;
    } else {
        shard.kafka_produce_failed++;
    }
}

void MetricsRegistry::observe_kafka_delivery(bool success) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (success) {
        shard.kafka_delivered++;
    } else {
        shard.kafka_delivery_failed++;
    }
}

void MetricsRegistry::observe_log_enqueue(bool accepted) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (accepted) {
        shard.log_enqueue_accepted++;
    } else {
        shard.log_enqueue_dropped++;
    }
}

void MetricsRegistry::observe_origin_cap(const std::string& kind, bool acquired) {
    Shard& shard = shard_for_this_thread();
    std::lock_guard<std::mutex> guard(shard.mutex);
    if (acquired) {
        shard.origin_cap_acquired[kind]++;
    } else {
        shard.origin_cap_rejected[kind]++;
    }
}

uint64_t MetricsRegistry::kafka_enqueue_dropped() const {
    uint64_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        total += shard.kafka_enqueue_dropped;
    }
    return total;
}

uint64_t MetricsRegistry::kafka_produce_accepted() const {
    uint64_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        total += shard.kafka_produce_accepted;
    }
    return total;
}

uint64_t MetricsRegistry::kafka_delivered() const {
    uint64_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        total += shard.kafka_delivered;
    }
    return total;
}

uint64_t MetricsRegistry::log_enqueue_dropped() const {
    uint64_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        total += shard.log_enqueue_dropped;
    }
    return total;
}

uint64_t MetricsRegistry::origin_cap_rejected(const std::string& kind) const {
    uint64_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        auto it = shard.origin_cap_rejected.find(kind);
        if (it != shard.origin_cap_rejected.end()) {
            total += it->second;
        }
    }
    return total;
}

void MetricsRegistry::reset_for_test() {
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        shard.http_requests.clear();
        shard.http_latency_buckets.fill(0);
        shard.http_latency_count = 0;
        shard.http_latency_sum = 0.0;
        shard.cache_results.clear();
        shard.kafka_publish_success = 0;
        shard.kafka_publish_failure = 0;
        shard.kafka_enqueue_accepted = 0;
        shard.kafka_enqueue_dropped = 0;
        shard.kafka_produce_accepted = 0;
        shard.kafka_produce_failed = 0;
        shard.kafka_delivered = 0;
        shard.kafka_delivery_failed = 0;
        shard.log_enqueue_accepted = 0;
        shard.log_enqueue_dropped = 0;
        shard.origin_cap_acquired.clear();
        shard.origin_cap_rejected.clear();
    }
}

std::string MetricsRegistry::render_prometheus() {
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

    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mutex);
        add_map(http_requests, shard.http_requests);
        for (size_t i = 0; i < http_latency_buckets.size(); ++i) {
            http_latency_buckets[i] += shard.http_latency_buckets[i];
        }
        http_latency_count += shard.http_latency_count;
        http_latency_sum += shard.http_latency_sum;
        add_map(cache_results, shard.cache_results);
        kafka_publish_success += shard.kafka_publish_success;
        kafka_publish_failure += shard.kafka_publish_failure;
        kafka_enqueue_accepted += shard.kafka_enqueue_accepted;
        kafka_enqueue_dropped += shard.kafka_enqueue_dropped;
        kafka_produce_accepted += shard.kafka_produce_accepted;
        kafka_produce_failed += shard.kafka_produce_failed;
        kafka_delivered += shard.kafka_delivered;
        kafka_delivery_failed += shard.kafka_delivery_failed;
        log_enqueue_accepted += shard.log_enqueue_accepted;
        log_enqueue_dropped += shard.log_enqueue_dropped;
        add_map(origin_cap_acquired, shard.origin_cap_acquired);
        add_map(origin_cap_rejected, shard.origin_cap_rejected);
    }

    std::ostringstream out;
    out << std::fixed << std::setprecision(6);

    out << "# HELP shorturl_http_requests_total Total HTTP requests by route and status class.\n";
    out << "# TYPE shorturl_http_requests_total counter\n";
    for (const auto& item : http_requests) {
        size_t first = item.first.find('|');
        size_t second = item.first.find('|', first + 1);
        std::map<std::string, std::string> label_values = {
            {"method", item.first.substr(0, first)},
            {"route", item.first.substr(first + 1, second - first - 1)},
            {"status_class", item.first.substr(second + 1)}
        };
        out << "shorturl_http_requests_total" << labels(label_values)
            << " " << item.second << "\n";
    }

    out << "# HELP shorturl_http_request_duration_seconds HTTP request latency histogram.\n";
    out << "# TYPE shorturl_http_request_duration_seconds histogram\n";
    for (size_t i = 0; i < sizeof(kLatencyBuckets) / sizeof(kLatencyBuckets[0]); ++i) {
        out << "shorturl_http_request_duration_seconds_bucket"
            << labels({{"le", std::to_string(kLatencyBuckets[i])}})
            << " " << http_latency_buckets[i] << "\n";
    }
    out << "shorturl_http_request_duration_seconds_bucket"
        << labels({{"le", "+Inf"}}) << " "
        << http_latency_buckets[http_latency_buckets.size() - 1] << "\n";
    out << "shorturl_http_request_duration_seconds_sum " << http_latency_sum << "\n";
    out << "shorturl_http_request_duration_seconds_count " << http_latency_count << "\n";

    out << "# HELP shorturl_cache_requests_total Cache lookups by result.\n";
    out << "# TYPE shorturl_cache_requests_total counter\n";
    for (const auto& item : cache_results) {
        out << "shorturl_cache_requests_total"
            << labels({{"result", item.first}})
            << " " << item.second << "\n";
    }

    out << "# HELP shorturl_kafka_publish_total Kafka click enqueue on request path "
           "(accepted vs dropped/unavailable; not zero-loss).\n";
    out << "# TYPE shorturl_kafka_publish_total counter\n";
    out << "shorturl_kafka_publish_total" << labels({{"result", "success"}})
        << " " << kafka_publish_success << "\n";
    out << "shorturl_kafka_publish_total" << labels({{"result", "failure"}})
        << " " << kafka_publish_failure << "\n";

    out << "# HELP shorturl_kafka_enqueue_total Bounded click-queue enqueue results.\n";
    out << "# TYPE shorturl_kafka_enqueue_total counter\n";
    out << "shorturl_kafka_enqueue_total" << labels({{"result", "accepted"}})
        << " " << kafka_enqueue_accepted << "\n";
    out << "shorturl_kafka_enqueue_total" << labels({{"result", "dropped"}})
        << " " << kafka_enqueue_dropped << "\n";

    out << "# HELP shorturl_kafka_produce_total Background rd_kafka_producev accept/fail.\n";
    out << "# TYPE shorturl_kafka_produce_total counter\n";
    out << "shorturl_kafka_produce_total" << labels({{"result", "accepted"}})
        << " " << kafka_produce_accepted << "\n";
    out << "shorturl_kafka_produce_total" << labels({{"result", "failed"}})
        << " " << kafka_produce_failed << "\n";

    out << "# HELP shorturl_kafka_delivery_total Kafka delivery-report callback outcomes.\n";
    out << "# TYPE shorturl_kafka_delivery_total counter\n";
    out << "shorturl_kafka_delivery_total" << labels({{"result", "success"}})
        << " " << kafka_delivered << "\n";
    out << "shorturl_kafka_delivery_total" << labels({{"result", "failure"}})
        << " " << kafka_delivery_failed << "\n";

    out << "# HELP shorturl_log_enqueue_total Bounded structured-log queue enqueue results.\n";
    out << "# TYPE shorturl_log_enqueue_total counter\n";
    out << "shorturl_log_enqueue_total" << labels({{"result", "accepted"}})
        << " " << log_enqueue_accepted << "\n";
    out << "shorturl_log_enqueue_total" << labels({{"result", "dropped"}})
        << " " << log_enqueue_dropped << "\n";

    out << "# HELP shorturl_origin_cap_total MySQL origin budget acquire results when Redis is down.\n";
    out << "# TYPE shorturl_origin_cap_total counter\n";
    for (const auto& item : origin_cap_acquired) {
        out << "shorturl_origin_cap_total"
            << labels({{"kind", item.first}, {"result", "acquired"}})
            << " " << item.second << "\n";
    }
    for (const auto& item : origin_cap_rejected) {
        out << "shorturl_origin_cap_total"
            << labels({{"kind", item.first}, {"result", "rejected"}})
            << " " << item.second << "\n";
    }

    return out.str();
}

std::string MetricsRegistry::labels(
    const std::map<std::string, std::string>& values) const {
    if (values.empty()) {
        return "";
    }
    std::ostringstream out;
    out << "{";
    bool first = true;
    for (const auto& item : values) {
        if (!first) {
            out << ",";
        }
        first = false;
        out << item.first << "=\"" << escape_label_value(item.second) << "\"";
    }
    out << "}";
    return out.str();
}

std::string MetricsRegistry::escape_label_value(const std::string& value) const {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        if (c == '\\' || c == '"') {
            escaped.push_back('\\');
        }
        escaped.push_back(c);
    }
    return escaped;
}
