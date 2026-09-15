#include "structured_logger.h"
#include "metrics_registry.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <sys/stat.h>

StructuredLogger& StructuredLogger::instance() {
    static StructuredLogger logger;
    return logger;
}

StructuredLogger::StructuredLogger()
    : enabled_(true), path_("./logs/access.jsonl"), max_queue_(8192),
      shutdown_timeout_ms_(1000), accepting_(false), stop_(true) {}

StructuredLogger::~StructuredLogger() {
    shutdown(shutdown_timeout_ms_);
}

void StructuredLogger::shutdown(int /*timeout_ms*/) {
    accepting_.store(false);
    stop_.store(true);
    cv_.notify_all();
    if (worker_.joinable()) {
        worker_.join();
    }
    {
        std::lock_guard<std::mutex> guard(mutex_);
        for (size_t i = 0; i < queue_.size(); ++i) {
            MetricsRegistry::instance().observe_log_enqueue(false);
        }
        queue_.clear();
    }
}

void StructuredLogger::init(const Config& config) {
    shutdown(config.structured_log_shutdown_timeout_ms);

    enabled_ = config.structured_log_enabled;
    path_ = config.structured_log_path;
    max_queue_ = static_cast<size_t>(std::max(1, config.structured_log_queue_size));
    shutdown_timeout_ms_ = std::max(0, config.structured_log_shutdown_timeout_ms);

    if (!path_.empty() && path_.compare(0, 7, "./logs/") == 0) {
        mkdir("./logs", 0755);
    }

    if (!enabled_ || path_.empty()) {
        return;
    }

    stop_.store(false);
    accepting_.store(true);
    worker_ = std::thread([this]() { worker_loop(); });
}

void StructuredLogger::init_for_test(size_t queue_size, const std::string& path,
                                     bool start_worker) {
    shutdown(100);
    enabled_ = true;
    path_ = path.empty() ? "/dev/null" : path;
    max_queue_ = std::max<size_t>(1, queue_size);
    shutdown_timeout_ms_ = 500;
    {
        std::lock_guard<std::mutex> guard(mutex_);
        queue_.clear();
    }
    stop_.store(!start_worker);
    accepting_.store(true);
    if (start_worker) {
        worker_ = std::thread([this]() { worker_loop(); });
    }
}

bool StructuredLogger::enqueue(std::string line) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!accepting_.load() || queue_.size() >= max_queue_) {
        return false;
    }
    queue_.push_back(std::move(line));
    cv_.notify_one();
    return true;
}

void StructuredLogger::log_access(const HttpRequest& req,
                                  int status,
                                  double duration_ms,
                                  const std::string& remote_addr) {
    if (!enabled_ || path_.empty() || !accepting_.load()) {
        return;
    }

    // Build owned line on the request thread; no ofstream here.
    std::ostringstream out;
    out << "{"
        << "\"ts\":\"" << now_iso8601() << "\","
        << "\"event\":\"http_access\","
        << "\"method\":\"" << json_escape(req.method) << "\","
        << "\"path\":\"" << json_escape(req.path) << "\","
        << "\"route\":\"" << json_escape(req.route_pattern) << "\","
        << "\"status\":" << status << ","
        << "\"duration_ms\":" << std::fixed << std::setprecision(3) << duration_ms << ","
        << "\"remote_addr\":\"" << json_escape(remote_addr) << "\","
        << "\"user_agent\":\"" << json_escape(req.header_or_empty("User-Agent")) << "\""
        << "}\n";

    if (!enqueue(out.str())) {
        MetricsRegistry::instance().observe_log_enqueue(false);
        return;
    }
    MetricsRegistry::instance().observe_log_enqueue(true);
}

void StructuredLogger::worker_loop() {
    std::ofstream out;
    out.open(path_.c_str(), std::ios::app);
    // Keep the file open for the life of the worker (no per-request reopen).

    while (true) {
        std::string line;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait(lock, [this]() {
                return stop_.load() || !queue_.empty();
            });
            if (stop_.load() && queue_.empty()) {
                break;
            }
            if (queue_.empty()) {
                continue;
            }
            line = std::move(queue_.front());
            queue_.pop_front();
        }

        if (!out.is_open()) {
            out.open(path_.c_str(), std::ios::app);
        }
        if (out) {
            out << line;
            out.flush();
        }
    }

    if (out.is_open()) {
        out.flush();
        out.close();
    }
}

std::string StructuredLogger::json_escape(const std::string& value) const {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        switch (c) {
            case '\\': escaped += "\\\\"; break;
            case '"': escaped += "\\\""; break;
            case '\n': escaped += "\\n"; break;
            case '\r': escaped += "\\r"; break;
            case '\t': escaped += "\\t"; break;
            default: escaped.push_back(c); break;
        }
    }
    return escaped;
}

std::string StructuredLogger::now_iso8601() const {
    using namespace std::chrono;
    const auto now = system_clock::now();
    const auto seconds = system_clock::to_time_t(now);
    const auto millis = duration_cast<milliseconds>(
        now.time_since_epoch()).count() % 1000;

    struct tm tm_value;
    localtime_r(&seconds, &tm_value);

    std::ostringstream out;
    out << std::put_time(&tm_value, "%Y-%m-%dT%H:%M:%S")
        << "." << std::setw(3) << std::setfill('0') << millis;
    return out.str();
}
