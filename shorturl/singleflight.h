#ifndef SHORTURL_SINGLEFLIGHT_H
#define SHORTURL_SINGLEFLIGHT_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

// Shared origin-lookup result for concurrent missers on the same key.
struct SingleFlightResult {
    enum class Kind {
        Hit,
        NotFound,
        Expired,
        Error,
        Overload
    };

    Kind kind = Kind::Error;
    std::string long_url;
    std::string expire_at;  // empty = never expires
    std::string error;
};

class SingleFlight {
public:
    SingleFlight();

    void configure(size_t max_inflight, size_t max_waiters_per_key);

    // One in-flight loader per key. Waiters share the loader result.
    // Overflow of in-flight keys or waiters returns Overload (→ 503).
    // The loader runs without holding the map lock or a MySQL connection
    // belonging to waiters (waiters block on condvar only).
    SingleFlightResult do_flight(const std::string& key,
                                 const std::function<SingleFlightResult()>& loader);

    // Test/observability helpers.
    size_t inflight_count() const;

private:
    struct Call {
        std::mutex mu;
        std::condition_variable cv;
        bool done = false;
        SingleFlightResult result;
        size_t waiters = 0;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<Call>> inflight_;
    size_t max_inflight_;
    size_t max_waiters_per_key_;
};

#endif
