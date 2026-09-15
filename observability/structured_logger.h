#ifndef OBSERVABILITY_STRUCTURED_LOGGER_H
#define OBSERVABILITY_STRUCTURED_LOGGER_H

#include "../config/config.h"
#include "../http/request.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <fstream>
#include <mutex>
#include <string>
#include <thread>

// Request path: build owned line, bounded enqueue. Background writer opens
// the file once (or keeps it open). Queue full → drop + metric.
class StructuredLogger {
public:
    static StructuredLogger& instance();

    void init(const Config& config);
    void shutdown(int timeout_ms = 1000);

    void log_access(const HttpRequest& req,
                    int status,
                    double duration_ms,
                    const std::string& remote_addr);

    // Test: tiny queue writing to path (or /dev/null).
    // start_worker=false keeps lines queued so overflow drops are observable.
    void init_for_test(size_t queue_size, const std::string& path,
                       bool start_worker = false);

private:
    StructuredLogger();
    ~StructuredLogger();

    void worker_loop();
    bool enqueue(std::string line);
    std::string json_escape(const std::string& value) const;
    std::string now_iso8601() const;

    bool enabled_;
    std::string path_;
    size_t max_queue_;
    int shutdown_timeout_ms_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<std::string> queue_;
    std::atomic<bool> accepting_;
    std::atomic<bool> stop_;
    std::thread worker_;
};

#endif
