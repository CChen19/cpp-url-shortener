#include "./config/config.h"
#include "webserver.h"
#include "./CGImysql/sql_connection_pool.h"
#include "./analytics/click_event_producer.h"
#include "./handler/health_handler.h"
#include "./handler/metrics_handler.h"
#include "./handler/short_url_handler.h"
#include "./observability/structured_logger.h"
#include "./shorturl/short_url_cache.h"
#include "./shorturl/shard_router.h"
#include "./shorturl/snowflake.h"

int main(int argc, char *argv[])
{
    Config config;

    std::string config_path = "config/config.yaml";
    if (argc > 1)
        config_path = argv[1];

    if (!config.load(config_path))
        fprintf(stderr, "Config load failed, using defaults\n");

    if (config.snowflake_worker_id >= 0)
    {
        // Pin the id-worker bits across both generator instances (short codes
        // and click event ids); default is pid-derived, which already keeps
        // same-host instances apart.
        SnowflakeIdGenerator::set_worker_id(config.snowflake_worker_id);
    }

    ShardRouter::instance().init(config);
    StructuredLogger::instance().init(config);
    ShortUrlCache::instance().init(config);
    ClickEventProducer::instance().init(config);
    init_short_url_handler(config);
    register_health_routes();
    register_metrics_routes();
    register_short_url_routes();

    WebServer server;
    server.init(config);

    server.log_write();
    server.sql_pool();
    ShortUrlCache::instance().warmup(connection_pool::GetInstance());
    server.thread_pool();
    server.trig_mode();
    server.eventListen();
    server.eventLoop();

    return 0;
}
