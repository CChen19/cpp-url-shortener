#include "click_event_producer.h"
#include "../observability/metrics_registry.h"
#include "../shorturl/base62.h"
#include "../shorturl/snowflake.h"
#include <nlohmann/json.hpp>
#include <algorithm>
#include <chrono>
#include <cstdio>

ClickEventProducer& ClickEventProducer::instance() {
    static ClickEventProducer producer;
    return producer;
}

ClickEventProducer::ClickEventProducer()
    : enabled_(false), available_(false), null_sink_(false), max_queue_(8192),
      shutdown_timeout_ms_(2000), accepting_(false), stop_(true)
#ifdef HAVE_RDKAFKA
      , producer_(nullptr)
#endif
{}

ClickEventProducer::~ClickEventProducer() {
    shutdown(shutdown_timeout_ms_);
}

void ClickEventProducer::shutdown(int timeout_ms) {
    accepting_.store(false);
    stop_.store(true);
    cv_.notify_all();

    if (worker_.joinable()) {
        // Bound join wait: worker exits on stop_ after current produce/poll.
        worker_.join();
    }

#ifdef HAVE_RDKAFKA
    if (producer_) {
        const int flush_ms = std::max(0, timeout_ms);
        rd_kafka_flush(producer_, flush_ms);
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
    }
#endif

    {
        std::lock_guard<std::mutex> guard(mutex_);
        if (!queue_.empty()) {
            MetricsRegistry::instance().observe_kafka_enqueue(false);
            // Count remaining as dropped on shutdown timeout/drop.
            for (size_t i = 1; i < queue_.size(); ++i) {
                MetricsRegistry::instance().observe_kafka_enqueue(false);
            }
            queue_.clear();
        }
    }

    available_ = false;
}

void ClickEventProducer::init(const Config& config) {
    shutdown(config.kafka_shutdown_timeout_ms);

    enabled_ = config.kafka_enabled;
    available_ = false;
    null_sink_ = false;
    topic_ = config.kafka_click_topic;
    max_queue_ = static_cast<size_t>(std::max(1, config.kafka_queue_size));
    shutdown_timeout_ms_ = std::max(0, config.kafka_shutdown_timeout_ms);
    stop_.store(false);
    accepting_.store(false);

    if (!enabled_) {
        return;
    }

#ifdef HAVE_RDKAFKA
    char errstr[512];
    rd_kafka_conf_t* conf = rd_kafka_conf_new();

    auto set_conf = [&](const char* key, const std::string& value) -> bool {
        rd_kafka_conf_res_t res =
            rd_kafka_conf_set(conf, key, value.c_str(), errstr, sizeof(errstr));
        return res == RD_KAFKA_CONF_OK;
    };

    bool ok = true;
    ok = ok && set_conf("bootstrap.servers", config.kafka_brokers);
    ok = ok && set_conf("acks", "all");
    ok = ok && set_conf("enable.idempotence", "true");
    ok = ok && set_conf("retries", std::to_string(config.kafka_retries));
    ok = ok && set_conf("message.timeout.ms",
                        std::to_string(config.kafka_message_timeout_ms));
    ok = ok && set_conf("linger.ms", std::to_string(config.kafka_linger_ms));

    if (!ok) {
        fprintf(stderr, "Kafka producer config error: %s\n", errstr);
        rd_kafka_conf_destroy(conf);
        return;
    }

    rd_kafka_conf_set_dr_msg_cb(conf, &ClickEventProducer::delivery_report_cb);
    rd_kafka_conf_set_opaque(conf, this);

    producer_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!producer_) {
        fprintf(stderr, "Kafka producer init error: %s\n", errstr);
        return;
    }

    available_ = true;
#else
    fprintf(stderr, "Kafka enabled but librdkafka not linked; clicks dropped\n");
    return;
#endif

    accepting_.store(true);
    stop_.store(false);
    worker_ = std::thread([this]() { worker_loop(); });
}

void ClickEventProducer::init_for_test(size_t queue_size, bool null_sink,
                                       bool start_worker) {
    shutdown(100);
    enabled_ = true;
    available_ = true;
    null_sink_ = null_sink;
    topic_ = "test.clicks";
    max_queue_ = std::max<size_t>(1, queue_size);
    shutdown_timeout_ms_ = 500;
#ifdef HAVE_RDKAFKA
    if (producer_) {
        rd_kafka_destroy(producer_);
        producer_ = nullptr;
    }
#endif
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

size_t ClickEventProducer::queue_size_for_test() const {
    // const cast for lock — size under mutex
    auto* self = const_cast<ClickEventProducer*>(this);
    std::lock_guard<std::mutex> guard(self->mutex_);
    return queue_.size();
}

bool ClickEventProducer::enqueue(ClickEvent event) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!accepting_.load() || queue_.size() >= max_queue_) {
        return false;
    }
    queue_.push_back(std::move(event));
    cv_.notify_one();
    return true;
}

bool ClickEventProducer::publish_click(const HttpRequest& req,
                                       const std::string& code,
                                       std::string* error) {
    if (!enabled_) {
        return false;
    }

    if (!accepting_.load()) {
        if (error) *error = "kafka producer shutting down";
        MetricsRegistry::instance().observe_kafka_enqueue(false);
        return false;
    }

    // Bound work on request path: copy fields only. No JSON, produce, poll,
    // or Snowflake (event_id is minted on the background worker).
    ClickEvent event;
    event.code = code;
    event.user_agent = header_or_empty(req, "User-Agent");
    event.referer = header_or_empty(req, "Referer");
    event.x_forwarded_for = header_or_empty(req, "X-Forwarded-For");
    event.clicked_at_ms = now_ms();

    if (!enqueue(std::move(event))) {
        if (error) *error = "kafka click queue full";
        MetricsRegistry::instance().observe_kafka_enqueue(false);
        return false;
    }

    MetricsRegistry::instance().observe_kafka_enqueue(true);
    return true;
}

void ClickEventProducer::worker_loop() {
    while (true) {
        ClickEvent event;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            // Time-bounded wait so the worker can rd_kafka_poll while idle.
            // librdkafka 1.x needs regular poll to drive produce I/O; poll(0)
            // only right after producev often expires messages (delivery fail)
            // even when the broker is up.
            const bool have_item = cv_.wait_for(
                lock, std::chrono::milliseconds(50), [this]() {
                    return stop_.load() || !queue_.empty();
                });
            if (stop_.load() && queue_.empty()) {
                break;
            }
            if (!have_item || queue_.empty()) {
                lock.unlock();
#ifdef HAVE_RDKAFKA
                if (producer_) {
                    rd_kafka_poll(producer_, 50);
                }
#endif
                continue;
            }
            event = std::move(queue_.front());
            queue_.pop_front();
        }

        if (event.event_id.empty()) {
            event.event_id = next_event_id();
        }

        if (null_sink_) {
            // Test / no-broker sink: count as produce-accepted without I/O.
            MetricsRegistry::instance().observe_kafka_produce_accepted(true);
            MetricsRegistry::instance().observe_kafka_delivery(true);
            continue;
        }

#ifdef HAVE_RDKAFKA
        if (!producer_) {
            MetricsRegistry::instance().observe_kafka_produce_accepted(false);
            continue;
        }

        const std::string payload = build_event_json(event);
        rd_kafka_resp_err_t err = rd_kafka_producev(
            producer_,
            RD_KAFKA_V_TOPIC(topic_.c_str()),
            RD_KAFKA_V_KEY(const_cast<char*>(event.code.data()), event.code.size()),
            RD_KAFKA_V_VALUE(const_cast<char*>(payload.data()), payload.size()),
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_END);

        if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
            fprintf(stderr, "Kafka produce error for click code=%s: %s\n",
                    event.code.c_str(), rd_kafka_err2str(err));
            MetricsRegistry::instance().observe_kafka_produce_accepted(false);
        } else {
            MetricsRegistry::instance().observe_kafka_produce_accepted(true);
        }
        // Background thread only. A short timeout lets 1.x librdkafka finish
        // the produce path; request threads still never poll.
        rd_kafka_poll(producer_, 10);
#else
        MetricsRegistry::instance().observe_kafka_produce_accepted(false);
#endif
    }

#ifdef HAVE_RDKAFKA
    if (producer_) {
        rd_kafka_poll(producer_, 0);
    }
#endif
}

#ifdef HAVE_RDKAFKA
void ClickEventProducer::delivery_report_cb(rd_kafka_t*,
                                            const rd_kafka_message_t* rkmessage,
                                            void*) {
    if (rkmessage->err) {
        MetricsRegistry::instance().observe_kafka_delivery(false);
    } else {
        MetricsRegistry::instance().observe_kafka_delivery(true);
    }
}
#endif

bool ClickEventProducer::enabled() const {
    return enabled_;
}

bool ClickEventProducer::available() const {
    return available_;
}

std::string ClickEventProducer::build_event_json(const ClickEvent& event) const {
    nlohmann::json body = {
        {"event_id", event.event_id},
        {"short_code", event.code},
        {"clicked_at_ms", event.clicked_at_ms},
        {"user_agent", event.user_agent},
        {"referer", event.referer},
        {"x_forwarded_for", event.x_forwarded_for}
    };
    return body.dump();
}

std::string ClickEventProducer::header_or_empty(const HttpRequest& req,
                                                const std::string& name) const {
    auto it = req.headers.find(name);
    return it == req.headers.end() ? "" : it->second;
}

std::string ClickEventProducer::next_event_id() {
    static SnowflakeIdGenerator generator;
    return base62_encode(generator.next_id());
}

long long ClickEventProducer::now_ms() const {
    using namespace std::chrono;
    return duration_cast<milliseconds>(
        system_clock::now().time_since_epoch()).count();
}
