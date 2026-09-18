#include "config.h"
#include <yaml-cpp/yaml.h>
#include <cstdio>

Config::Config()
    : port(9006), thread_num(8), trig_mode(0), opt_linger(false), actor_model(0),
      public_base_url("http://localhost:9006"),
      log_enabled(true), log_async(false), log_path("./ServerLog"),
      mysql_host("localhost"), mysql_port(3306), mysql_user("shorturl"),
      mysql_password("shorturl"), mysql_database("shorturl"), mysql_pool_size(8),
      mysql_acquire_timeout_ms(50),
      redis_enabled(true), redis_uri("tcp://127.0.0.1:6379"),
      redis_connect_timeout_ms(200), redis_socket_timeout_ms(200),
      redis_pool_size(8), redis_probe_interval_ms(1000),
      cache_ttl_seconds(3600), cache_ttl_jitter_seconds(300),
      bloom_bits(1048576), bloom_hashes(7), bloom_hard_filter(false),
      local_cache_shards(16),
      local_positive_bytes(64 * 1024 * 1024),
      local_negative_bytes(4 * 1024 * 1024),
      local_negative_ttl_seconds(30),
      singleflight_max_inflight(1024),
      singleflight_max_waiters_per_key(256),
      origin_redirect_max_concurrent(64),
      origin_create_max_concurrent(32),
      kafka_enabled(true), kafka_brokers("127.0.0.1:9092"),
      kafka_click_topic("shorturl.clicks"), kafka_message_timeout_ms(3000),
      kafka_linger_ms(5), kafka_retries(3),
      kafka_queue_size(8192), kafka_shutdown_timeout_ms(2000),
      sharding_enabled(true), shard_database_prefix("shorturl_"),
      shard_table_prefix("short_url_"), shard_database_count(4),
      shard_table_count(4),
      snowflake_worker_id(-1),
      structured_log_enabled(true), structured_log_path("./logs/access.jsonl"),
      structured_log_queue_size(8192), structured_log_shutdown_timeout_ms(1000),
      close_log(0)
{}

bool Config::load(const std::string& path)
{
    // Load-or-nothing: snapshot before any mutation so a YAML error part-way
    // through cannot leave a half-parsed mix of defaults and overrides behind
    // (main() would then run on that mix while claiming "using defaults").
    const Config rollback = *this;
    try {
        YAML::Node cfg = YAML::LoadFile(path);

        if (cfg["server"]) {
            auto s = cfg["server"];
            if (s["port"])        port        = s["port"].as<int>();
            if (s["thread_num"])  thread_num  = s["thread_num"].as<int>();
            if (s["trig_mode"])   trig_mode   = s["trig_mode"].as<int>();
            if (s["opt_linger"])  opt_linger  = s["opt_linger"].as<bool>();
            if (s["actor_model"]) actor_model = s["actor_model"].as<int>();
            if (s["public_base_url"]) {
                public_base_url = s["public_base_url"].as<std::string>();
            }
        }

        if (cfg["log"]) {
            auto l = cfg["log"];
            if (l["enabled"]) {
                log_enabled = l["enabled"].as<bool>();
                close_log = log_enabled ? 0 : 1;
            }
            if (l["async"]) log_async = l["async"].as<bool>();
            if (l["path"])  log_path  = l["path"].as<std::string>();
        }

        if (cfg["mysql"]) {
            auto m = cfg["mysql"];
            if (m["host"])      mysql_host     = m["host"].as<std::string>();
            if (m["port"])      mysql_port     = m["port"].as<int>();
            if (m["user"])      mysql_user     = m["user"].as<std::string>();
            if (m["password"])  mysql_password = m["password"].as<std::string>();
            if (m["database"])  mysql_database = m["database"].as<std::string>();
            if (m["pool_size"]) mysql_pool_size = m["pool_size"].as<int>();
            if (m["acquire_timeout_ms"]) {
                mysql_acquire_timeout_ms = m["acquire_timeout_ms"].as<int>();
            }
        }

        if (cfg["redis"]) {
            auto r = cfg["redis"];
            if (r["enabled"])            redis_enabled            = r["enabled"].as<bool>();
            if (r["uri"])                redis_uri                = r["uri"].as<std::string>();
            if (r["connect_timeout_ms"]) redis_connect_timeout_ms = r["connect_timeout_ms"].as<int>();
            if (r["socket_timeout_ms"])  redis_socket_timeout_ms  = r["socket_timeout_ms"].as<int>();
            if (r["pool_size"])          redis_pool_size          = r["pool_size"].as<int>();
            if (r["probe_interval_ms"])  redis_probe_interval_ms  = r["probe_interval_ms"].as<int>();
        }

        if (cfg["cache"]) {
            auto c = cfg["cache"];
            if (c["ttl_seconds"])        cache_ttl_seconds        = c["ttl_seconds"].as<int>();
            if (c["ttl_jitter_seconds"]) cache_ttl_jitter_seconds = c["ttl_jitter_seconds"].as<int>();
            if (c["bloom_bits"])         bloom_bits               = c["bloom_bits"].as<int>();
            if (c["bloom_hashes"])       bloom_hashes             = c["bloom_hashes"].as<int>();
            if (c["bloom_hard_filter"])  bloom_hard_filter        = c["bloom_hard_filter"].as<bool>();
            if (c["local_shards"])       local_cache_shards       = c["local_shards"].as<int>();
            if (c["local_positive_bytes"]) {
                local_positive_bytes = c["local_positive_bytes"].as<int>();
            }
            if (c["local_negative_bytes"]) {
                local_negative_bytes = c["local_negative_bytes"].as<int>();
            }
            if (c["local_negative_ttl_seconds"]) {
                local_negative_ttl_seconds = c["local_negative_ttl_seconds"].as<int>();
            }
            if (c["singleflight_max_inflight"]) {
                singleflight_max_inflight = c["singleflight_max_inflight"].as<int>();
            }
            if (c["singleflight_max_waiters_per_key"]) {
                singleflight_max_waiters_per_key =
                    c["singleflight_max_waiters_per_key"].as<int>();
            }
            if (c["origin_redirect_max_concurrent"]) {
                origin_redirect_max_concurrent =
                    c["origin_redirect_max_concurrent"].as<int>();
            }
            if (c["origin_create_max_concurrent"]) {
                origin_create_max_concurrent =
                    c["origin_create_max_concurrent"].as<int>();
            }
        }

        if (cfg["kafka"]) {
            auto k = cfg["kafka"];
            if (k["enabled"])            kafka_enabled            = k["enabled"].as<bool>();
            if (k["brokers"])            kafka_brokers            = k["brokers"].as<std::string>();
            if (k["click_topic"])        kafka_click_topic        = k["click_topic"].as<std::string>();
            if (k["message_timeout_ms"]) kafka_message_timeout_ms = k["message_timeout_ms"].as<int>();
            if (k["linger_ms"])          kafka_linger_ms          = k["linger_ms"].as<int>();
            if (k["retries"])            kafka_retries            = k["retries"].as<int>();
            if (k["queue_size"])         kafka_queue_size         = k["queue_size"].as<int>();
            if (k["shutdown_timeout_ms"]) {
                kafka_shutdown_timeout_ms = k["shutdown_timeout_ms"].as<int>();
            }
        }

        if (cfg["sharding"]) {
            auto sh = cfg["sharding"];
            if (sh["enabled"])         sharding_enabled     = sh["enabled"].as<bool>();
            if (sh["database_prefix"]) shard_database_prefix = sh["database_prefix"].as<std::string>();
            if (sh["table_prefix"])    shard_table_prefix    = sh["table_prefix"].as<std::string>();
            if (sh["database_count"])  shard_database_count  = sh["database_count"].as<int>();
            if (sh["table_count"])     shard_table_count     = sh["table_count"].as<int>();
        }

        if (cfg["snowflake"]) {
            auto sf = cfg["snowflake"];
            if (sf["worker_id"]) snowflake_worker_id = sf["worker_id"].as<int>();
        }

        if (cfg["observability"]) {
            auto obs = cfg["observability"];
            if (obs["structured_log_enabled"]) {
                structured_log_enabled = obs["structured_log_enabled"].as<bool>();
            }
            if (obs["structured_log_path"]) {
                structured_log_path = obs["structured_log_path"].as<std::string>();
            }
            if (obs["structured_log_queue_size"]) {
                structured_log_queue_size = obs["structured_log_queue_size"].as<int>();
            }
            if (obs["structured_log_shutdown_timeout_ms"]) {
                structured_log_shutdown_timeout_ms =
                    obs["structured_log_shutdown_timeout_ms"].as<int>();
            }
        }

        return true;
    } catch (const YAML::Exception& e) {
        fprintf(stderr, "Failed to load config '%s': %s\n", path.c_str(), e.what());
        *this = rollback;
        return false;
    }
}
