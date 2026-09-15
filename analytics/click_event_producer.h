#ifndef ANALYTICS_CLICK_EVENT_PRODUCER_H
#define ANALYTICS_CLICK_EVENT_PRODUCER_H

#include "../config/config.h"
#include "../http/request.h"
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

#ifdef HAVE_RDKAFKA
#include <librdkafka/rdkafka.h>
#endif

// Request path: copy fields into an owned event and bounded-enqueue (never
// blocks the 302). Background thread: JSON serialize, produce, poll.
// Queue full → drop + metric. Not zero-loss.
class ClickEventProducer {
public:
    static ClickEventProducer& instance();

    void init(const Config& config);
    // Stop accepting, drain/drop with timeout, join worker. Must not hang shutdown.
    void shutdown(int timeout_ms = 2000);

    bool publish_click(const HttpRequest& req, const std::string& code,
                       std::string* error = nullptr);
    bool enabled() const;
    bool available() const;

    // Test: tiny queue, optional null sink (no librdkafka). Counts drops.
    // When start_worker is false, enqueue-only mode (for drop-counter tests).
    void init_for_test(size_t queue_size, bool null_sink = true,
                       bool start_worker = false);

    size_t queue_size_for_test() const;

private:
    struct ClickEvent {
        std::string code;
        std::string user_agent;
        std::string referer;
        std::string x_forwarded_for;
        std::string event_id;
        long long clicked_at_ms = 0;
    };

    ClickEventProducer();
    ~ClickEventProducer();

    void worker_loop();
    bool enqueue(ClickEvent event);
    std::string build_event_json(const ClickEvent& event) const;
    std::string header_or_empty(const HttpRequest& req, const std::string& name) const;
    std::string next_event_id();
    long long now_ms() const;

#ifdef HAVE_RDKAFKA
    static void delivery_report_cb(rd_kafka_t* rk,
                                   const rd_kafka_message_t* rkmessage,
                                   void* opaque);
#endif

    bool enabled_;
    bool available_;
    bool null_sink_;
    std::string topic_;
    size_t max_queue_;
    int shutdown_timeout_ms_;

    std::mutex mutex_;
    std::condition_variable cv_;
    std::deque<ClickEvent> queue_;
    std::atomic<bool> accepting_;
    std::atomic<bool> stop_;
    std::thread worker_;

#ifdef HAVE_RDKAFKA
    rd_kafka_t* producer_;
#endif
};

#endif
