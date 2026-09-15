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
            if (expire_at.empty()) {
                ShortUrlCache::instance().set(record.short_code, long_url);
            } else {
                ShortUrlCache::instance().add_legal_code(record.short_code);
            }

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

void redirect(const HttpRequest& req, HttpResponse& resp) {
    auto it = req.params.find("code");
    if (it == req.params.end() || it->second.empty()) {
        resp.set_status(404);
        resp.set_json({{"error", "not found"}});
        return;
    }

    const std::string code = it->second;
    ShortUrlCache& cache = ShortUrlCache::instance();
    std::string long_url;
    std::string cache_error;

    ShortUrlCache::CacheStatus cache_status =
        cache.get(code, &long_url, &cache_error);
    if (cache_status == ShortUrlCache::CacheStatus::Hit) {
        ClickEventProducer::instance().publish_click(req, code);
        set_safe_location(resp, long_url);
        return;
    }
    if (cache_status == ShortUrlCache::CacheStatus::Filtered) {
        resp.set_status(404);
        resp.set_json({{"error", "short url not found"}});
        return;
    }

    std::shared_ptr<std::mutex> rebuild_lock = cache.rebuild_mutex(code);
    std::lock_guard<std::mutex> guard(*rebuild_lock);

    cache_status = cache.get(code, &long_url, &cache_error);
    if (cache_status == ShortUrlCache::CacheStatus::Hit) {
        ClickEventProducer::instance().publish_click(req, code);
        set_safe_location(resp, long_url);
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

    ShortUrlRepository repo(mysql);
    std::string db_error;
    bool cacheable = false;
    ShortUrlRepository::FindStatus status =
        repo.find_long_url(code, &long_url, &db_error, &cacheable);

    if (status == ShortUrlRepository::FindStatus::Ok) {
        if (cacheable) {
            cache.set(code, long_url);
        } else {
            cache.add_legal_code(code);
        }
        ClickEventProducer::instance().publish_click(req, code);
        set_safe_location(resp, long_url);
        return;
    }
    if (status == ShortUrlRepository::FindStatus::Expired) {
        cache.erase(code);
        resp.set_status(410);
        resp.set_json({{"error", "short url expired"}});
        return;
    }
    if (status == ShortUrlRepository::FindStatus::NotFound) {
        resp.set_status(404);
        resp.set_json({{"error", "short url not found"}});
        return;
    }

    resp.set_status(503);
    resp.set_json({{"error", "short url storage unavailable"}, {"detail", db_error}});
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
