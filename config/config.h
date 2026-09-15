#ifndef CONFIG_H
#define CONFIG_H

#include <string>

class Config
{
public:
    Config();

    bool load(const std::string& path);

    int port;
    int thread_num;
    int trig_mode;
    bool opt_linger;
    int actor_model;

    // Public base used when minting short_url (not derived from Host).
    std::string public_base_url;

    bool log_enabled;
    bool log_async;
    std::string log_path;

    std::string mysql_host;
    int mysql_port;
    std::string mysql_user;
    std::string mysql_password;
    std::string mysql_database;
    int mysql_pool_size;
    // Max wait when borrowing a pool connection (ms). Default 50.
    int mysql_acquire_timeout_ms;

    bool redis_enabled;
    std::string redis_uri;
    int redis_connect_timeout_ms;
    int redis_socket_timeout_ms;
    int redis_pool_size;
    int redis_probe_interval_ms;
    int cache_ttl_seconds;
    int cache_ttl_jitter_seconds;
    int bloom_bits;
    int bloom_hashes;
    // When true, bloom miss is a hard 404. Default false: bloom is a hint only.
    // Opt-in true is single-process-only; expired codes loaded at warmup can 404 vs 410.
    bool bloom_hard_filter;
    int local_cache_shards;
    int local_positive_bytes;
    int local_negative_bytes;
    int local_negative_ttl_seconds;
    int singleflight_max_inflight;
    int singleflight_max_waiters_per_key;
    // Concurrent MySQL origin budgets while Redis is unavailable (separate so
    // create cannot starve redirect refill).
    int origin_redirect_max_concurrent;
    int origin_create_max_concurrent;

    bool kafka_enabled;
    std::string kafka_brokers;
    std::string kafka_click_topic;
    int kafka_message_timeout_ms;
    int kafka_linger_ms;
    int kafka_retries;
    int kafka_queue_size;
    int kafka_shutdown_timeout_ms;

    bool sharding_enabled;
    std::string shard_database_prefix;
    std::string shard_table_prefix;
    int shard_database_count;
    int shard_table_count;

    bool structured_log_enabled;
    std::string structured_log_path;
    int structured_log_queue_size;
    int structured_log_shutdown_timeout_ms;

    int close_log;
};

#endif
