#include "short_url_cache.h"
#include "short_url_repository.h"
#include "../observability/metrics_registry.h"
#include <algorithm>
#include <cstdlib>
#include <ctime>
#include <vector>

namespace {

#ifdef HAVE_REDIS_PLUS_PLUS
void fill_redis_options(const std::string& uri,
                        sw::redis::ConnectionOptions* options) {
    std::string endpoint = uri;
    const std::string tcp_prefix = "tcp://";
    const std::string redis_prefix = "redis://";

    if (endpoint.compare(0, tcp_prefix.size(), tcp_prefix) == 0) {
        endpoint = endpoint.substr(tcp_prefix.size());
    } else if (endpoint.compare(0, redis_prefix.size(), redis_prefix) == 0) {
        endpoint = endpoint.substr(redis_prefix.size());
    }

    const std::string::size_type slash = endpoint.find('/');
    if (slash != std::string::npos) {
        endpoint = endpoint.substr(0, slash);
    }

    const std::string::size_type colon = endpoint.rfind(':');
    if (colon == std::string::npos) {
        options->host = endpoint.empty() ? "127.0.0.1" : endpoint;
        return;
    }

    options->host = colon == 0 ? "127.0.0.1" : endpoint.substr(0, colon);
    const std::string port = endpoint.substr(colon + 1);
    if (!port.empty()) {
        options->port = std::atoi(port.c_str());
    }
}
#endif

} // namespace

// ---------------------------------------------------------------------------
// LocalUrlCache
// ---------------------------------------------------------------------------

LocalUrlCache::LocalUrlCache()
    : shard_count_(16),
      positive_byte_budget_(64 * 1024 * 1024),
      negative_byte_budget_(4 * 1024 * 1024),
      positive_ttl_seconds_(3600),
      positive_ttl_jitter_seconds_(300),
      negative_ttl_seconds_(30),
      rng_(static_cast<unsigned>(
          std::chrono::steady_clock::now().time_since_epoch().count())) {
    configure(shard_count_, positive_byte_budget_, negative_byte_budget_,
              positive_ttl_seconds_, positive_ttl_jitter_seconds_,
              negative_ttl_seconds_);
}

void LocalUrlCache::configure(size_t shard_count,
                              size_t positive_byte_budget,
                              size_t negative_byte_budget,
                              int positive_ttl_seconds,
                              int positive_ttl_jitter_seconds,
                              int negative_ttl_seconds) {
    shard_count_ = std::max<size_t>(1, shard_count);
    // Configured budgets are process-wide; split evenly across shards.
    positive_byte_budget_ =
        std::max<size_t>(1024, positive_byte_budget / shard_count_);
    negative_byte_budget_ =
        std::max<size_t>(1024, negative_byte_budget / shard_count_);
    positive_ttl_seconds_ = std::max(1, positive_ttl_seconds);
    positive_ttl_jitter_seconds_ = std::max(0, positive_ttl_jitter_seconds);
    negative_ttl_seconds_ = std::max(1, negative_ttl_seconds);

    shards_.clear();
    shards_.reserve(shard_count_);
    for (size_t i = 0; i < shard_count_; ++i) {
        shards_.emplace_back(new Shard());
    }
}

std::string LocalUrlCache::now_datetime() {
    std::time_t t = std::time(nullptr);
    std::tm tm_buf;
#if defined(_WIN32)
    localtime_s(&tm_buf, &t);
#else
    localtime_r(&t, &tm_buf);
#endif
    char buf[20];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm_buf);
    return std::string(buf);
}

bool LocalUrlCache::is_business_expired(const std::string& expire_at) {
    if (expire_at.empty()) {
        return false;
    }
    return expire_at <= now_datetime();
}

size_t LocalUrlCache::shard_index(const std::string& code) const {
    // FNV-1a 64-bit then reduce.
    uint64_t h = 1469598103934665603ULL;
    for (unsigned char c : code) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    return static_cast<size_t>(h % shard_count_);
}

size_t LocalUrlCache::estimate_positive_bytes(const std::string& code,
                                              const PositiveEntry& entry) {
    return code.size() + entry.long_url.size() + entry.expire_at.size() + 64;
}

size_t LocalUrlCache::estimate_negative_bytes(const std::string& code) {
    return code.size() + 32;
}

int LocalUrlCache::ttl_with_jitter_locked() {
    if (positive_ttl_jitter_seconds_ <= 0) {
        return positive_ttl_seconds_;
    }
    std::uniform_int_distribution<int> dist(0, positive_ttl_jitter_seconds_);
    return positive_ttl_seconds_ + dist(rng_);
}

void LocalUrlCache::evict_positive_locked(Shard& shard) {
    while (shard.positive_bytes > positive_byte_budget_ &&
           !shard.positive_fifo.empty()) {
        const std::string victim = shard.positive_fifo.front();
        shard.positive_fifo.pop_front();
        auto it = shard.positive.find(victim);
        if (it == shard.positive.end()) {
            continue;
        }
        shard.positive_bytes -= it->second.bytes;
        shard.positive.erase(it);
    }
}

void LocalUrlCache::evict_negative_locked(Shard& shard) {
    while (shard.negative_bytes > negative_byte_budget_ &&
           !shard.negative_fifo.empty()) {
        const std::string victim = shard.negative_fifo.front();
        shard.negative_fifo.pop_front();
        auto it = shard.negative.find(victim);
        if (it == shard.negative.end()) {
            continue;
        }
        shard.negative_bytes -= it->second.bytes;
        shard.negative.erase(it);
    }
}

LocalUrlCache::Status LocalUrlCache::get(const std::string& code,
                                         EntryView* out) {
    Shard& shard = *shards_[shard_index(code)];
    std::lock_guard<std::mutex> guard(shard.mutex);
    const auto now = std::chrono::steady_clock::now();

    auto pit = shard.positive.find(code);
    if (pit != shard.positive.end()) {
        if (pit->second.cache_expire <= now) {
            shard.positive_bytes -= pit->second.bytes;
            shard.positive.erase(pit);
            // Cache TTL miss → refill; fall through to negative check then Miss.
        } else if (is_business_expired(pit->second.expire_at)) {
            if (out) {
                out->long_url = pit->second.long_url;
                out->expire_at = pit->second.expire_at;
            }
            shard.positive_bytes -= pit->second.bytes;
            shard.positive.erase(pit);
            return Status::Expired;
        } else {
            if (out) {
                out->long_url = pit->second.long_url;
                out->expire_at = pit->second.expire_at;
            }
            return Status::Hit;
        }
    }

    auto nit = shard.negative.find(code);
    if (nit != shard.negative.end()) {
        if (nit->second.cache_expire <= now) {
            shard.negative_bytes -= nit->second.bytes;
            shard.negative.erase(nit);
            return Status::Miss;
        }
        return Status::NegHit;
    }

    return Status::Miss;
}

void LocalUrlCache::put_positive(const std::string& code,
                                 const std::string& long_url,
                                 const std::string& expire_at) {
    Shard& shard = *shards_[shard_index(code)];
    std::lock_guard<std::mutex> guard(shard.mutex);

    auto nit = shard.negative.find(code);
    if (nit != shard.negative.end()) {
        shard.negative_bytes -= nit->second.bytes;
        shard.negative.erase(nit);
    }

    PositiveEntry entry;
    entry.long_url = long_url;
    entry.expire_at = expire_at;
    int ttl;
    {
        std::lock_guard<std::mutex> rng_guard(rng_mutex_);
        ttl = ttl_with_jitter_locked();
    }
    entry.cache_expire =
        std::chrono::steady_clock::now() + std::chrono::seconds(ttl);
    entry.bytes = estimate_positive_bytes(code, entry);

    auto pit = shard.positive.find(code);
    if (pit != shard.positive.end()) {
        shard.positive_bytes -= pit->second.bytes;
        pit->second = entry;
        shard.positive_bytes += entry.bytes;
    } else {
        shard.positive.emplace(code, entry);
        shard.positive_fifo.push_back(code);
        shard.positive_bytes += entry.bytes;
    }
    evict_positive_locked(shard);
}

void LocalUrlCache::put_negative(const std::string& code) {
    Shard& shard = *shards_[shard_index(code)];
    std::lock_guard<std::mutex> guard(shard.mutex);

    auto pit = shard.positive.find(code);
    if (pit != shard.positive.end()) {
        shard.positive_bytes -= pit->second.bytes;
        shard.positive.erase(pit);
    }

    NegativeEntry entry;
    entry.cache_expire = std::chrono::steady_clock::now() +
                         std::chrono::seconds(negative_ttl_seconds_);
    entry.bytes = estimate_negative_bytes(code);

    auto nit = shard.negative.find(code);
    if (nit != shard.negative.end()) {
        shard.negative_bytes -= nit->second.bytes;
        nit->second = entry;
        shard.negative_bytes += entry.bytes;
    } else {
        shard.negative.emplace(code, entry);
        shard.negative_fifo.push_back(code);
        shard.negative_bytes += entry.bytes;
    }
    evict_negative_locked(shard);
}

void LocalUrlCache::erase(const std::string& code) {
    Shard& shard = *shards_[shard_index(code)];
    std::lock_guard<std::mutex> guard(shard.mutex);

    auto pit = shard.positive.find(code);
    if (pit != shard.positive.end()) {
        shard.positive_bytes -= pit->second.bytes;
        shard.positive.erase(pit);
    }
    auto nit = shard.negative.find(code);
    if (nit != shard.negative.end()) {
        shard.negative_bytes -= nit->second.bytes;
        shard.negative.erase(nit);
    }
}

size_t LocalUrlCache::positive_bytes() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard->mutex);
        total += shard->positive_bytes;
    }
    return total;
}

size_t LocalUrlCache::negative_bytes() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard->mutex);
        total += shard->negative_bytes;
    }
    return total;
}

// ---------------------------------------------------------------------------
// ShortUrlCache
// ---------------------------------------------------------------------------

ShortUrlCache& ShortUrlCache::instance() {
    static ShortUrlCache cache;
    return cache;
}

ShortUrlCache::ShortUrlCache()
    : enabled_(false), redis_available_(false), bloom_ready_(false),
      bloom_hard_filter_(false), ttl_seconds_(3600), ttl_jitter_seconds_(300),
      bloom_bits_(1048576), bloom_hashes_(7),
      rng_(static_cast<unsigned>(
          std::chrono::steady_clock::now().time_since_epoch().count())) {}

void ShortUrlCache::init(const Config& config) {
    std::lock_guard<std::mutex> guard(mutex_);

    enabled_ = config.redis_enabled;
    redis_available_ = false;
    bloom_ready_ = false;
    bloom_hard_filter_ = config.bloom_hard_filter;
    ttl_seconds_ = std::max(1, config.cache_ttl_seconds);
    ttl_jitter_seconds_ = std::max(0, config.cache_ttl_jitter_seconds);
    bloom_bits_ = std::max(8, config.bloom_bits);
    bloom_hashes_ = std::max(1, config.bloom_hashes);
    bloom_.reset(bloom_bits_, bloom_hashes_);

    local_.configure(static_cast<size_t>(std::max(1, config.local_cache_shards)),
                     static_cast<size_t>(std::max(1024, config.local_positive_bytes)),
                     static_cast<size_t>(std::max(1024, config.local_negative_bytes)),
                     ttl_seconds_,
                     ttl_jitter_seconds_,
                     std::max(1, config.local_negative_ttl_seconds));

    singleflight_.configure(
        static_cast<size_t>(std::max(1, config.singleflight_max_inflight)),
        static_cast<size_t>(std::max(1, config.singleflight_max_waiters_per_key)));

    if (!enabled_) {
        return;
    }

#ifdef HAVE_REDIS_PLUS_PLUS
    try {
        sw::redis::ConnectionOptions options;
        fill_redis_options(config.redis_uri, &options);
        options.connect_timeout = std::chrono::milliseconds(config.redis_connect_timeout_ms);
        options.socket_timeout = std::chrono::milliseconds(config.redis_socket_timeout_ms);
        redis_.reset(new sw::redis::Redis(options));
        redis_->ping();
        redis_available_ = true;
    } catch (const sw::redis::Error&) {
        redis_.reset();
        redis_available_ = false;
    }
#endif
}

bool ShortUrlCache::warmup(connection_pool* pool, std::string* error) {
    MYSQL* mysql = nullptr;
    if (pool) {
        mysql = pool->GetConnection();
    }
    if (!mysql) {
        if (error) *error = "mysql connection unavailable";
        return false;
    }

    ShortUrlRepository repo(mysql);
    std::vector<std::string> codes;
    const bool ok = repo.list_active_codes(&codes, error);
    pool->ReleaseConnection(mysql);
    if (!ok) {
        return false;
    }

    for (const std::string& code : codes) {
        bloom_.add(code);
    }
    bloom_ready_ = true;
    return true;
}

ShortUrlCache::CacheStatus
ShortUrlCache::get_local(const std::string& code, LookupValue* value) {
    LocalUrlCache::EntryView view;
    const LocalUrlCache::Status st = local_.get(code, &view);
    if (st == LocalUrlCache::Status::Hit) {
        if (value) {
            value->long_url = view.long_url;
            value->expire_at = view.expire_at;
        }
        MetricsRegistry::instance().observe_cache_result("local_hit");
        return CacheStatus::Hit;
    }
    if (st == LocalUrlCache::Status::Expired) {
        if (value) {
            value->long_url = view.long_url;
            value->expire_at = view.expire_at;
        }
        MetricsRegistry::instance().observe_cache_result("local_expired");
        return CacheStatus::Expired;
    }
    if (st == LocalUrlCache::Status::NegHit) {
        MetricsRegistry::instance().observe_cache_result("local_neg");
        return CacheStatus::NotFound;
    }
    MetricsRegistry::instance().observe_cache_result("local_miss");
    return CacheStatus::Miss;
}

ShortUrlCache::CacheStatus
ShortUrlCache::get_redis(const std::string& code, LookupValue* value,
                         std::string* error) {
    if (!enabled_) {
        MetricsRegistry::instance().observe_cache_result("redis_unavailable");
        return CacheStatus::Unavailable;
    }

#ifdef HAVE_REDIS_PLUS_PLUS
    std::lock_guard<std::mutex> guard(mutex_);
    if (!redis_available_ || !redis_) {
        MetricsRegistry::instance().observe_cache_result("redis_unavailable");
        return CacheStatus::Unavailable;
    }

    try {
        auto raw = redis_->get(cache_key(code));
        if (!raw) {
            MetricsRegistry::instance().observe_cache_result("redis_miss");
            return CacheStatus::Miss;
        }

        const CacheStatus decoded = interpret_redis_payload(*raw, value);
        if (decoded == CacheStatus::Miss) {
            // Undecodable / legacy plain URL: cannot carry expire_at → refill MySQL.
            MetricsRegistry::instance().observe_cache_result("redis_undecodable");
            return CacheStatus::Miss;
        }
        if (decoded == CacheStatus::Expired) {
            try {
                redis_->del(cache_key(code));
            } catch (const sw::redis::Error&) {
                // Best-effort drop of expired mapping.
            }
            MetricsRegistry::instance().observe_cache_result("redis_expired");
            return CacheStatus::Expired;
        }
        MetricsRegistry::instance().observe_cache_result("redis_hit");
        return CacheStatus::Hit;
    } catch (const sw::redis::Error& e) {
        redis_available_ = false;
        if (error) *error = e.what();
        MetricsRegistry::instance().observe_cache_result("redis_unavailable");
        return CacheStatus::Unavailable;
    }
#else
    (void)code;
    (void)value;
    (void)error;
    MetricsRegistry::instance().observe_cache_result("redis_unavailable");
    return CacheStatus::Unavailable;
#endif
}

bool ShortUrlCache::set(const std::string& code, const std::string& long_url,
                        const std::string& expire_at, std::string* error) {
    add_legal_code(code);
    put_local_positive(code, long_url, expire_at);

    if (!enabled_) {
        return false;
    }

#ifdef HAVE_REDIS_PLUS_PLUS
    std::lock_guard<std::mutex> guard(mutex_);
    if (!redis_available_ || !redis_) {
        return false;
    }

    try {
        redis_->setex(cache_key(code), ttl_with_jitter(),
                      encode_redis_value(long_url, expire_at));
        return true;
    } catch (const sw::redis::Error& e) {
        redis_available_ = false;
        if (error) *error = e.what();
        return false;
    }
#else
    (void)error;
    return false;
#endif
}

bool ShortUrlCache::erase(const std::string& code, std::string* error) {
    erase_local(code);

    if (!enabled_) {
        return false;
    }

#ifdef HAVE_REDIS_PLUS_PLUS
    std::lock_guard<std::mutex> guard(mutex_);
    if (!redis_available_ || !redis_) {
        return false;
    }

    try {
        redis_->del(cache_key(code));
        return true;
    } catch (const sw::redis::Error& e) {
        redis_available_ = false;
        if (error) *error = e.what();
        return false;
    }
#else
    (void)error;
    return false;
#endif
}

void ShortUrlCache::put_local_positive(const std::string& code,
                                       const std::string& long_url,
                                       const std::string& expire_at) {
    local_.put_positive(code, long_url, expire_at);
}

void ShortUrlCache::put_local_negative(const std::string& code) {
    local_.put_negative(code);
}

void ShortUrlCache::erase_local(const std::string& code) {
    local_.erase(code);
}

void ShortUrlCache::add_legal_code(const std::string& code) {
    bloom_.add(code);
}

bool ShortUrlCache::bloom_might_contain(const std::string& code) const {
    return bloom_.might_contain(code);
}

bool ShortUrlCache::bloom_hard_filter() const {
    return bloom_hard_filter_;
}

bool ShortUrlCache::bloom_ready() const {
    return bloom_ready_;
}

SingleFlight& ShortUrlCache::singleflight() {
    return singleflight_;
}

LocalUrlCache& ShortUrlCache::local_cache() {
    return local_;
}

const LocalUrlCache& ShortUrlCache::local_cache() const {
    return local_;
}

bool ShortUrlCache::enabled() const {
    return enabled_;
}

bool ShortUrlCache::redis_available() const {
    return redis_available_;
}

int ShortUrlCache::ttl_with_jitter() {
    if (ttl_jitter_seconds_ <= 0) {
        return ttl_seconds_;
    }
    std::uniform_int_distribution<int> dist(0, ttl_jitter_seconds_);
    return ttl_seconds_ + dist(rng_);
}

std::string ShortUrlCache::cache_key(const std::string& code) const {
    return "shorturl:" + code;
}

std::string ShortUrlCache::encode_redis_value(const std::string& long_url,
                                              const std::string& expire_at) {
    // v1\n<expire_at>\n<long_url>  — expire_at may be empty (never).
    return std::string("v1\n") + expire_at + "\n" + long_url;
}

bool ShortUrlCache::decode_redis_value(const std::string& raw,
                                       std::string* long_url,
                                       std::string* expire_at) {
    if (raw.compare(0, 3, "v1\n") != 0) {
        return false;
    }
    const std::string::size_type second = raw.find('\n', 3);
    if (second == std::string::npos) {
        return false;
    }
    if (expire_at) {
        *expire_at = raw.substr(3, second - 3);
    }
    if (long_url) {
        *long_url = raw.substr(second + 1);
    }
    return true;
}

ShortUrlCache::CacheStatus
ShortUrlCache::interpret_redis_payload(const std::string& raw,
                                       LookupValue* value) {
    std::string long_url;
    std::string expire_at;
    if (!decode_redis_value(raw, &long_url, &expire_at)) {
        // Phase-1 plain URLs / garbage cannot carry business expiry → Miss → MySQL.
        return CacheStatus::Miss;
    }
    if (LocalUrlCache::is_business_expired(expire_at)) {
        if (value) {
            value->long_url = long_url;
            value->expire_at = expire_at;
        }
        return CacheStatus::Expired;
    }
    if (value) {
        value->long_url = long_url;
        value->expire_at = expire_at;
    }
    return CacheStatus::Hit;
}
