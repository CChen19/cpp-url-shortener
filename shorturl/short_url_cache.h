#ifndef SHORTURL_SHORT_URL_CACHE_H
#define SHORTURL_SHORT_URL_CACHE_H

#include "bloom_filter.h"
#include "singleflight.h"
#include "../config/config.h"
#include "../CGImysql/sql_connection_pool.h"
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef HAVE_REDIS_PLUS_PLUS
#include <sw/redis++/redis++.h>
#endif

// Sharded in-process cache. Values carry business expiry separately from
// cache TTL. Byte budgets (not only entry counts) bound memory.
class LocalUrlCache {
public:
    enum class Status {
        Hit,
        Expired,   // within cache TTL but past business expire_at → 410
        Miss,
        NegHit     // negative cache → treat as not found
    };

    struct EntryView {
        std::string long_url;
        std::string expire_at;
    };

    LocalUrlCache();

    void configure(size_t shard_count,
                   size_t positive_byte_budget,
                   size_t negative_byte_budget,
                   int positive_ttl_seconds,
                   int positive_ttl_jitter_seconds,
                   int negative_ttl_seconds);

    // May drop TTL-expired or business-expired entries (lazy eviction).
    Status get(const std::string& code, EntryView* out);
    void put_positive(const std::string& code,
                      const std::string& long_url,
                      const std::string& expire_at);
    void put_negative(const std::string& code);
    void erase(const std::string& code);

    size_t positive_bytes() const;
    size_t negative_bytes() const;

    // Wall-clock "YYYY-MM-DD HH:MM:SS" comparable to MySQL expire_at strings.
    static std::string now_datetime();
    static bool is_business_expired(const std::string& expire_at);

private:
    struct PositiveEntry {
        std::string long_url;
        std::string expire_at;
        std::chrono::steady_clock::time_point cache_expire;
        size_t bytes = 0;
    };

    struct NegativeEntry {
        std::chrono::steady_clock::time_point cache_expire;
        size_t bytes = 0;
    };

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, PositiveEntry> positive;
        std::deque<std::string> positive_fifo;
        size_t positive_bytes = 0;
        std::unordered_map<std::string, NegativeEntry> negative;
        std::deque<std::string> negative_fifo;
        size_t negative_bytes = 0;
    };

    size_t shard_index(const std::string& code) const;
    static size_t estimate_positive_bytes(const std::string& code,
                                          const PositiveEntry& entry);
    static size_t estimate_negative_bytes(const std::string& code);
    int ttl_with_jitter_locked();
    void evict_positive_locked(Shard& shard);
    void evict_negative_locked(Shard& shard);

    size_t shard_count_;
    size_t positive_byte_budget_;
    size_t negative_byte_budget_;
    int positive_ttl_seconds_;
    int positive_ttl_jitter_seconds_;
    int negative_ttl_seconds_;
    mutable std::mutex rng_mutex_;
    mutable std::mt19937 rng_;
    std::vector<std::unique_ptr<Shard>> shards_;
};

class ShortUrlCache {
public:
    enum class CacheStatus {
        Hit,
        Expired,      // business expiry → 410
        Miss,
        NotFound,     // negative L1 hit
        Filtered,     // bloom hard-filter only
        Unavailable,
        Overload
    };

    struct LookupValue {
        std::string long_url;
        std::string expire_at;
    };

    static ShortUrlCache& instance();

    void init(const Config& config);
    bool warmup(connection_pool* pool, std::string* error = nullptr);

    // L1 only: never talks to Redis/MySQL. Used by handler before singleflight.
    CacheStatus get_local(const std::string& code, LookupValue* value);

    // Redis L2 get/set. Values encode expire_at. Global mutex remains (phase 3).
    CacheStatus get_redis(const std::string& code, LookupValue* value,
                          std::string* error = nullptr);
    bool set(const std::string& code, const std::string& long_url,
             const std::string& expire_at = std::string(),
             std::string* error = nullptr);
    bool erase(const std::string& code, std::string* error = nullptr);

    void put_local_positive(const std::string& code,
                            const std::string& long_url,
                            const std::string& expire_at);
    void put_local_negative(const std::string& code);
    void erase_local(const std::string& code);

    void add_legal_code(const std::string& code);
    bool bloom_might_contain(const std::string& code) const;
    bool bloom_hard_filter() const;
    bool bloom_ready() const;

    SingleFlight& singleflight();
    LocalUrlCache& local_cache();
    const LocalUrlCache& local_cache() const;

    bool enabled() const;
    bool redis_available() const;

    // Redis wire format. Undecodable payloads are not Hits (no expire_at → Miss/refill).
    static std::string encode_redis_value(const std::string& long_url,
                                          const std::string& expire_at);
    static bool decode_redis_value(const std::string& raw,
                                   std::string* long_url,
                                   std::string* expire_at);
    // Classify a Redis GET payload without I/O. Plain/legacy/garbage → Miss.
    static CacheStatus interpret_redis_payload(const std::string& raw,
                                               LookupValue* value);

private:
    ShortUrlCache();

    int ttl_with_jitter();
    std::string cache_key(const std::string& code) const;

    bool enabled_;
    bool redis_available_;
    bool bloom_ready_;
    bool bloom_hard_filter_;
    int ttl_seconds_;
    int ttl_jitter_seconds_;
    int bloom_bits_;
    int bloom_hashes_;

    mutable std::mutex mutex_;
    std::mt19937 rng_;
    BloomFilter bloom_;
    SingleFlight singleflight_;
    LocalUrlCache local_;

#ifdef HAVE_REDIS_PLUS_PLUS
    std::unique_ptr<sw::redis::Redis> redis_;
#endif
};

#endif
