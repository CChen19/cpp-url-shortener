cmake_minimum_required(VERSION 3.14)
project(TinyWebServer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(yaml-cpp REQUIRED)
find_path(REDIS_PLUS_PLUS_INCLUDE_DIR sw/redis++/redis++.h)
find_library(REDIS_PLUS_PLUS_LIBRARY NAMES redis++)
find_library(HIREDIS_LIBRARY NAMES hiredis)
find_path(RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h)
find_library(RDKAFKA_LIBRARY NAMES rdkafka)

include_directories(
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/third_party
)

set(SOURCES
    main.cpp
    webserver.cpp
    analytics/click_event_producer.cpp
    config/config.cpp
    log/log.cpp
    CGImysql/sql_connection_pool.cpp
    timer/lst_timer.cpp
    http/http_conn.cpp
    http/router.cpp
    http/response.cpp
    handler/health_handler.cpp
    handler/metrics_handler.cpp
    handler/short_url_handler.cpp
    observability/metrics_registry.cpp
    observability/structured_logger.cpp
    shorturl/base62.cpp
    shorturl/bloom_filter.cpp
    shorturl/singleflight.cpp
    shorturl/shard_router.cpp
    shorturl/snowflake.cpp
    shorturl/short_url_cache.cpp
    shorturl/short_url_repository.cpp
)

add_executable(server ${SOURCES})

if (REDIS_PLUS_PLUS_INCLUDE_DIR AND REDIS_PLUS_PLUS_LIBRARY AND HIREDIS_LIBRARY)
    target_compile_definitions(server PRIVATE HAVE_REDIS_PLUS_PLUS)
    target_include_directories(server PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIR})
    target_link_libraries(server
        ${REDIS_PLUS_PLUS_LIBRARY}
        ${HIREDIS_LIBRARY}
    )
else()
    message(WARNING "redis-plus-plus or hiredis not found; Redis cache layer will build in DB-only fallback mode")
endif()

if (RDKAFKA_INCLUDE_DIR AND RDKAFKA_LIBRARY)
    target_compile_definitions(server PRIVATE HAVE_RDKAFKA)
    target_include_directories(server PRIVATE ${RDKAFKA_INCLUDE_DIR})
    target_link_libraries(server ${RDKAFKA_LIBRARY})
else()
    message(WARNING "librdkafka not found; Kafka click producer will build in no-op fallback mode")
endif()

target_link_libraries(server
    Threads::Threads
    mysqlclient
    yaml-cpp
)

enable_testing()

add_executable(test_mysql_pool_timeout
    tests/test_mysql_pool_timeout.cpp
    CGImysql/sql_connection_pool.cpp
    log/log.cpp
)

target_link_libraries(test_mysql_pool_timeout
    Threads::Threads
    mysqlclient
)

add_test(NAME mysql_pool_timeout COMMAND test_mysql_pool_timeout)

add_executable(test_conn_generation
    tests/test_conn_generation.cpp
)

target_link_libraries(test_conn_generation
    Threads::Threads
)

add_test(NAME conn_generation COMMAND test_conn_generation)

add_executable(test_http_protocol
    tests/test_http_protocol.cpp
)

add_test(NAME http_protocol COMMAND test_http_protocol)

add_executable(test_local_cache
    tests/test_local_cache.cpp
    shorturl/short_url_cache.cpp
    shorturl/singleflight.cpp
    shorturl/bloom_filter.cpp
    shorturl/short_url_repository.cpp
    shorturl/shard_router.cpp
    config/config.cpp
    observability/metrics_registry.cpp
    CGImysql/sql_connection_pool.cpp
    log/log.cpp
)

target_link_libraries(test_local_cache
    Threads::Threads
    mysqlclient
    yaml-cpp
)

if (REDIS_PLUS_PLUS_INCLUDE_DIR AND REDIS_PLUS_PLUS_LIBRARY AND HIREDIS_LIBRARY)
    target_compile_definitions(test_local_cache PRIVATE HAVE_REDIS_PLUS_PLUS)
    target_include_directories(test_local_cache PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIR})
    target_link_libraries(test_local_cache
        ${REDIS_PLUS_PLUS_LIBRARY}
        ${HIREDIS_LIBRARY}
    )
endif()

add_test(NAME local_cache COMMAND test_local_cache)

add_executable(test_phase3_isolate
    tests/test_phase3_isolate.cpp
    analytics/click_event_producer.cpp
    observability/structured_logger.cpp
    observability/metrics_registry.cpp
    shorturl/short_url_cache.cpp
    shorturl/singleflight.cpp
    shorturl/bloom_filter.cpp
    shorturl/short_url_repository.cpp
    shorturl/shard_router.cpp
    shorturl/snowflake.cpp
    shorturl/base62.cpp
    config/config.cpp
    CGImysql/sql_connection_pool.cpp
    log/log.cpp
)

target_link_libraries(test_phase3_isolate
    Threads::Threads
    mysqlclient
    yaml-cpp
)

if (REDIS_PLUS_PLUS_INCLUDE_DIR AND REDIS_PLUS_PLUS_LIBRARY AND HIREDIS_LIBRARY)
    target_compile_definitions(test_phase3_isolate PRIVATE HAVE_REDIS_PLUS_PLUS)
    target_include_directories(test_phase3_isolate PRIVATE ${REDIS_PLUS_PLUS_INCLUDE_DIR})
    target_link_libraries(test_phase3_isolate
        ${REDIS_PLUS_PLUS_LIBRARY}
        ${HIREDIS_LIBRARY}
    )
endif()

if (RDKAFKA_INCLUDE_DIR AND RDKAFKA_LIBRARY)
    target_compile_definitions(test_phase3_isolate PRIVATE HAVE_RDKAFKA)
    target_include_directories(test_phase3_isolate PRIVATE ${RDKAFKA_INCLUDE_DIR})
    target_link_libraries(test_phase3_isolate ${RDKAFKA_LIBRARY})
endif()

add_test(NAME phase3_isolate COMMAND test_phase3_isolate)

add_executable(test_metrics_shards
    tests/test_metrics_shards.cpp
    observability/metrics_registry.cpp
)
target_link_libraries(test_metrics_shards Threads::Threads)
add_test(NAME metrics_shards COMMAND test_metrics_shards)

# In-process observe contention microbench (not a ctest; run manually for evidence).
add_executable(bench_metrics_observe
    tests/bench_metrics_observe.cpp
    observability/metrics_registry.cpp
)
target_link_libraries(bench_metrics_observe Threads::Threads)
