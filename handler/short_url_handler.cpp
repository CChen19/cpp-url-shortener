#include "short_url_handler.h"
#include "../analytics/click_event_producer.h"
#include "../CGImysql/sql_connection_pool.h"
#include "../config/config.h"
#include "../http/protocol_utils.h"
#include "../http/router.h"
#include "../shorturl/base62.h"
#include "../shorturl/short_url_cache.h"
#include "../shorturl/short_url_repository.h"
#include "../shorturl/snowflake.h"
#include <nlohmann/json.hpp>
#include <cctype>

namespace {

std::string g_public_base_url = "http://localhost:9006";

bool valid_expire_at(const std::string& expire_at) {
    if (expire_at.size() != 19) {
        return false;
    }
    for (size_t i = 0; i < expire_at.size(); ++i) {
        if (i == 4 || i == 7) {
            if (expire_at[i] != '-') return false;
        } else if (i == 10) {
            if (expire_at[i] != ' ') return false;
        } else if (i == 13 || i == 16) {
            if (expire_at[i] != ':') return false;
        } else if (!std::isdigit(static_cast<unsigned char>(expire_at[i]))) {
            return false;
        }
    }
    return true;
}

std::string build_short_url(const std::string& code) {
    return join_public_base_url(g_public_base_url, code);
}

bool set_safe_location(HttpResponse& resp, const std::string& long_url) {
    if (!is_http_url_safe_for_location(long_url)) {
        resp.set_status(500);
        resp.set_json({{"error", "stored long_url is unsafe for Location"}});
        return false;
    }
    resp.set_status(302);
    resp.set_header("Location", long_url);
    resp.set_body("");
    return true;
}

void respond_expired(HttpResponse& resp) {
    resp.set_status(410);
    resp.set_json({{"error", "short url expired"}});
}

void respond_not_found(HttpResponse& resp) {
    resp.set_status(404);
    resp.set_json({{"error", "short url not found"}});
}

void shorten(const HttpRequest& req, HttpResponse& resp) {
    nlohmann::json payload;
    try {
        payload = nlohmann::json::parse(req.body);
    } catch (const nlohmann::json::parse_error&) {
        resp.set_status(400);
        resp.set_json({{"error", "invalid json"}});
        return;
    }

    if (!payload.is_object() || !payload.contains("long_url") ||
        !payload["long_url"].is_string()) {
        resp.set_status(400);
        resp.set_json({{"error", "long_url is required"}});
        return;
    }

    const std::string long_url = payload["long_url"].get<std::string>();
    if (!is_http_url_safe_for_location(long_url)) {
        resp.set_status(400);
        resp.set_json({{"error",
            "long_url must be http(s) without control characters"}});
        return;
    }

    std::string expire_at;
    if (payload.contains("expire_at") && !payload["expire_at"].is_null()) {
        if (!payload["expire_at"].is_string()) {
            resp.set_status(400);
            resp.set_json({{"error", "expire_at must be a datetime string"}});
            return;
        }
        expire_at = payload["expire_at"].get<std::string>();
        if (!valid_expire_at(expire_at)) {
            resp.set_status(400);
            resp.set_json({{"error", "expire_at format must be YYYY-MM-DD HH:MM:SS"}});
            return;
        }
    }

    // Separate create budget so create cannot starve redirect refill when Redis is down.
    OriginBudgetGuard create_budget =
        ShortUrlCache::instance().try_acquire_origin(ShortUrlCache::OriginKind::Create);
    if (!ShortUrlCache::instance().redis_available() && !create_budget.held()) {
        resp.set_status(503);
        resp.set_json({{"error", "short url storage unavailable"},
                       {"detail", "create origin budget exhausted"}});
        return;
    }

    MYSQL* mysql = nullptr;
    connectionRAII mysqlcon(&mysql, connection_pool::GetInstance());
    if (!mysql) {
        resp.set_status(503);
        resp.set_json({{"error", "short url storage unavailable"},
                       {"detail", "mysql connection unavailable"}});
        return;
    }

    static SnowflakeIdGenerator generator;
    ShortUrlRepository repo(mysql);

    std::string db_error;
    for (int i = 0; i < 3; ++i) {
        ShortUrlRecord record;
        record.id = generator.next_id();
        record.short_code = base62_encode(record.id);
        record.long_url = long_url;
        record.expire_at = expire_at;

        ShortUrlRepository::CreateStatus status = repo.create(record, &db_error);
        if (status == ShortUrlRepository::CreateStatus::Ok) {
            // Always write L1 (+ Redis if up) with expire_at. Cache TTL ≠ business expiry.
            ShortUrlCache::instance().set(record.short_code, long_url, expire_at);

            resp.set_status(201);
            nlohmann::json body = {
                {"short_code", record.short_code},
                {"short_url", build_short_url(record.short_code)},
                {"long_url", long_url}
            };
            body["expire_at"] = expire_at.empty() ? nlohmann::json(nullptr) : nlohmann::json(expire_at);
            resp.set_json(body);
            return;
        }
        if (status != ShortUrlRepository::CreateStatus::DuplicateCode) {
            break;
        }
    }

    resp.set_status(503);
    resp.set_json({{"error", "short url storage unavailable"}, {"detail", db_error}});
}

SingleFlightResult origin_lookup(ShortUrlCache& cache, const std::string& code) {
    SingleFlightResult out;

    // Cap MySQL origin when Redis is down; excess → Overload/503 (not unbounded DB).
    OriginBudgetGuard redirect_budget =
        cache.try_acquire_origin(ShortUrlCache::OriginKind::Redirect);
    if (!cache.redis_available() && !redirect_budget.held()) {
        out.kind = SingleFlightResult::Kind::Overload;
        out.error = "redirect origin budget exhausted";
        return out;
    }

    ShortUrlCache::LookupValue cached;
    ShortUrlCache::CacheStatus redis_status =
        cache.get_redis(code, &cached);
    if (redis_status == ShortUrlCache::CacheStatus::Hit) {
        cache.put_local_positive(code, cached.long_url, cached.expire_at);
        out.kind = SingleFlightResult::Kind::Hit;
        out.long_url = cached.long_url;
        out.expire_at = cached.expire_at;
        return out;
    }
    if (redis_status == ShortUrlCache::CacheStatus::Expired) {
        cache.erase_local(code);
        out.kind = SingleFlightResult::Kind::Expired;
        out.long_url = cached.long_url;
        out.expire_at = cached.expire_at;
        return out;
    }

    // Redis miss/unavailable: refill from MySQL. Redis I/O is not under app mutex.
    MYSQL* mysql = nullptr;
    connectionRAII mysqlcon(&mysql, connection_pool::GetInstance());
    if (!mysql) {
        out.kind = SingleFlightResult::Kind::Error;
        out.error = "mysql connection unavailable";
        return out;
    }

    ShortUrlRepository repo(mysql);
    std::string db_error;
    std::string long_url;
    std::string expire_at;
    ShortUrlRepository::FindStatus status =
        repo.find_long_url(code, &long_url, &db_error, &expire_at);

    if (status == ShortUrlRepository::FindStatus::Ok) {
        cache.set(code, long_url, expire_at);
        out.kind = SingleFlightResult::Kind::Hit;
        out.long_url = long_url;
        out.expire_at = expire_at;
        return out;
    }
    if (status == ShortUrlRepository::FindStatus::Expired) {
        cache.erase(code);
        out.kind = SingleFlightResult::Kind::Expired;
        out.long_url = long_url;
        out.expire_at = expire_at;
        return out;
    }
    if (status == ShortUrlRepository::FindStatus::NotFound) {
        cache.put_local_negative(code);
        out.kind = SingleFlightResult::Kind::NotFound;
        return out;
    }

    out.kind = SingleFlightResult::Kind::Error;
    out.error = db_error;
    return out;
}

void redirect(const HttpRequest& req, HttpResponse& resp) {
    auto it = req.params.find("code");
    if (it == req.params.end() || it->second.empty()) {
        respond_not_found(resp);
        return;
    }

    const std::string code = it->second;
    ShortUrlCache& cache = ShortUrlCache::instance();

    // Bloom is a hint by default. Hard 404 only when bloom_hard_filter is on.
    if (cache.bloom_hard_filter() && cache.bloom_ready() &&
        !cache.bloom_might_contain(code)) {
        respond_not_found(resp);
        return;
    }

    ShortUrlCache::LookupValue local_value;
    ShortUrlCache::CacheStatus local_status = cache.get_local(code, &local_value);
    if (local_status == ShortUrlCache::CacheStatus::Hit) {
        ClickEventProducer::instance().publish_click(req, code);
        set_safe_location(resp, local_value.long_url);
        return;
    }
    if (local_status == ShortUrlCache::CacheStatus::Expired) {
        // Business expiry on L1: never 302. Drop positive; do not negative-cache
        // as NotFound (that would flip 410 → 404).
        cache.erase(code);
        respond_expired(resp);
        return;
    }
    if (local_status == ShortUrlCache::CacheStatus::NotFound) {
        respond_not_found(resp);
        return;
    }

    // Miss: one shared origin lookup (Redis → MySQL). Waiters share the result.
    // Waiters sit on condvar only; MySQL is acquired inside the loader, not while waiting.
    SingleFlightResult flight = cache.singleflight().do_flight(
        code, [&]() { return origin_lookup(cache, code); });

    if (flight.kind == SingleFlightResult::Kind::Hit) {
        ClickEventProducer::instance().publish_click(req, code);
        set_safe_location(resp, flight.long_url);
        return;
    }
    if (flight.kind == SingleFlightResult::Kind::Expired) {
        respond_expired(resp);
        return;
    }
    if (flight.kind == SingleFlightResult::Kind::NotFound) {
        respond_not_found(resp);
        return;
    }
    if (flight.kind == SingleFlightResult::Kind::Overload) {
        resp.set_status(503);
        resp.set_json({{"error", "short url storage unavailable"},
                       {"detail", flight.error.empty() ? "singleflight overload"
                                                       : flight.error}});
        return;
    }

    resp.set_status(503);
    resp.set_json({{"error", "short url storage unavailable"},
                   {"detail", flight.error}});
}

} // namespace

void init_short_url_handler(const Config& cfg) {
    if (!cfg.public_base_url.empty()) {
        g_public_base_url = cfg.public_base_url;
    }
}

void register_short_url_routes() {
    Router::instance().post("/api/shorten", shorten);
    Router::instance().get("/{code}", redirect);
}
