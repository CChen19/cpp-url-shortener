#include "metrics_registry.h"
#include <iomanip>
#include <sstream>

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

} // namespace

MetricsRegistry& MetricsRegistry::instance() {
    static MetricsRegistry registry;
    return registry;
}

MetricsRegistry::MetricsRegistry()
    : http_latency_buckets_(), http_latency_count_(0), http_latency_sum_(0.0),
      kafka_publish_success_(0), kafka_publish_failure_(0),
      kafka_enqueue_accepted_(0), kafka_enqueue_dropped_(0),
      kafka_produce_accepted_(0), kafka_produce_failed_(0),
      kafka_delivered_(0), kafka_delivery_failed_(0),
      log_enqueue_accepted_(0), log_enqueue_dropped_(0) {}

void MetricsRegistry::observe_http_request(const std::string& method,
                                           const std::string& route,
                                           int status,
                                           double duration_seconds) {
    std::lock_guard<std::mutex> guard(mutex_);
    const std::string key = method + "|" + route + "|" + status_class(status);
    http_requests_[key]++;
    for (size_t i = 0; i < sizeof(kLatencyBuckets) / sizeof(kLatencyBuckets[0]); ++i) {
        if (duration_seconds <= kLatencyBuckets[i]) {
            http_latency_buckets_[i]++;
        }
    }
    http_latency_buckets_[http_latency_buckets_.size() - 1]++;
    http_latency_count_++;
    http_latency_sum_ += duration_seconds;
}

void MetricsRegistry::observe_cache_result(const std::string& result) {
    std::lock_guard<std::mutex> guard(mutex_);
    cache_results_[result]++;
}

void MetricsRegistry::observe_kafka_publish(bool success) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (success) {
        kafka_publish_success_++;
    } else {
        kafka_publish_failure_++;
    }
}

void MetricsRegistry::observe_kafka_enqueue(bool accepted) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (accepted) {
        kafka_enqueue_accepted_++;
        kafka_publish_success_++;
    } else {
        kafka_enqueue_dropped_++;
        kafka_publish_failure_++;
    }
}

void MetricsRegistry::observe_kafka_produce_accepted(bool ok) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (ok) {
        kafka_produce_accepted_++;
    } else {
        kafka_produce_failed_++;
    }
}

void MetricsRegistry::observe_kafka_delivery(bool success) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (success) {
        kafka_delivered_++;
    } else {
        kafka_delivery_failed_++;
    }
}

void MetricsRegistry::observe_log_enqueue(bool accepted) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (accepted) {
        log_enqueue_accepted_++;
    } else {
        log_enqueue_dropped_++;
    }
}

void MetricsRegistry::observe_origin_cap(const std::string& kind, bool acquired) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (acquired) {
        origin_cap_acquired_[kind]++;
    } else {
        origin_cap_rejected_[kind]++;
    }
}

uint64_t MetricsRegistry::kafka_enqueue_dropped() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return kafka_enqueue_dropped_;
}

uint64_t MetricsRegistry::kafka_produce_accepted() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return kafka_produce_accepted_;
}

uint64_t MetricsRegistry::kafka_delivered() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return kafka_delivered_;
}

uint64_t MetricsRegistry::log_enqueue_dropped() const {
    std::lock_guard<std::mutex> guard(mutex_);
    return log_enqueue_dropped_;
}

uint64_t MetricsRegistry::origin_cap_rejected(const std::string& kind) const {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = origin_cap_rejected_.find(kind);
    return it == origin_cap_rejected_.end() ? 0 : it->second;
}

void MetricsRegistry::reset_for_test() {
    std::lock_guard<std::mutex> guard(mutex_);
    http_requests_.clear();
    http_latency_buckets_.fill(0);
    http_latency_count_ = 0;
    http_latency_sum_ = 0.0;
    cache_results_.clear();
    kafka_publish_success_ = 0;
    kafka_publish_failure_ = 0;
    kafka_enqueue_accepted_ = 0;
    kafka_enqueue_dropped_ = 0;
    kafka_produce_accepted_ = 0;
    kafka_produce_failed_ = 0;
    kafka_delivered_ = 0;
    kafka_delivery_failed_ = 0;
    log_enqueue_accepted_ = 0;
    log_enqueue_dropped_ = 0;
    origin_cap_acquired_.clear();
    origin_cap_rejected_.clear();
}

std::string MetricsRegistry::render_prometheus() {
    std::lock_guard<std::mutex> guard(mutex_);
    std::ostringstream out;
    out << std::fixed << std::setprecision(6);

    out << "# HELP shorturl_http_requests_total Total HTTP requests by route and status class.\n";
    out << "# TYPE shorturl_http_requests_total counter\n";
    for (const auto& item : http_requests_) {
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
            << " " << http_latency_buckets_[i] << "\n";
    }
    out << "shorturl_http_request_duration_seconds_bucket"
        << labels({{"le", "+Inf"}}) << " "
        << http_latency_buckets_[http_latency_buckets_.size() - 1] << "\n";
    out << "shorturl_http_request_duration_seconds_sum " << http_latency_sum_ << "\n";
    out << "shorturl_http_request_duration_seconds_count " << http_latency_count_ << "\n";

    out << "# HELP shorturl_cache_requests_total Cache lookups by result.\n";
    out << "# TYPE shorturl_cache_requests_total counter\n";
    for (const auto& item : cache_results_) {
        out << "shorturl_cache_requests_total"
            << labels({{"result", item.first}})
            << " " << item.second << "\n";
    }

    out << "# HELP shorturl_kafka_publish_total Kafka click enqueue on request path "
           "(accepted vs dropped/unavailable; not zero-loss).\n";
    out << "# TYPE shorturl_kafka_publish_total counter\n";
    out << "shorturl_kafka_publish_total" << labels({{"result", "success"}})
        << " " << kafka_publish_success_ << "\n";
    out << "shorturl_kafka_publish_total" << labels({{"result", "failure"}})
        << " " << kafka_publish_failure_ << "\n";

    out << "# HELP shorturl_kafka_enqueue_total Bounded click-queue enqueue results.\n";
    out << "# TYPE shorturl_kafka_enqueue_total counter\n";
    out << "shorturl_kafka_enqueue_total" << labels({{"result", "accepted"}})
        << " " << kafka_enqueue_accepted_ << "\n";
    out << "shorturl_kafka_enqueue_total" << labels({{"result", "dropped"}})
        << " " << kafka_enqueue_dropped_ << "\n";

    out << "# HELP shorturl_kafka_produce_total Background rd_kafka_producev accept/fail.\n";
    out << "# TYPE shorturl_kafka_produce_total counter\n";
    out << "shorturl_kafka_produce_total" << labels({{"result", "accepted"}})
        << " " << kafka_produce_accepted_ << "\n";
    out << "shorturl_kafka_produce_total" << labels({{"result", "failed"}})
        << " " << kafka_produce_failed_ << "\n";

    out << "# HELP shorturl_kafka_delivery_total Kafka delivery-report callback outcomes.\n";
    out << "# TYPE shorturl_kafka_delivery_total counter\n";
    out << "shorturl_kafka_delivery_total" << labels({{"result", "success"}})
        << " " << kafka_delivered_ << "\n";
    out << "shorturl_kafka_delivery_total" << labels({{"result", "failure"}})
        << " " << kafka_delivery_failed_ << "\n";

    out << "# HELP shorturl_log_enqueue_total Bounded structured-log queue enqueue results.\n";
    out << "# TYPE shorturl_log_enqueue_total counter\n";
    out << "shorturl_log_enqueue_total" << labels({{"result", "accepted"}})
        << " " << log_enqueue_accepted_ << "\n";
    out << "shorturl_log_enqueue_total" << labels({{"result", "dropped"}})
        << " " << log_enqueue_dropped_ << "\n";

    out << "# HELP shorturl_origin_cap_total MySQL origin budget acquire results when Redis is down.\n";
    out << "# TYPE shorturl_origin_cap_total counter\n";
    for (const auto& item : origin_cap_acquired_) {
        out << "shorturl_origin_cap_total"
            << labels({{"kind", item.first}, {"result", "acquired"}})
            << " " << item.second << "\n";
    }
    for (const auto& item : origin_cap_rejected_) {
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
