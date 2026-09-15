#include "../shorturl/short_url_cache.h"
#include "../shorturl/singleflight.h"
#include "../shorturl/bloom_filter.h"
#include "../config/config.h"
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

namespace {

int g_failures = 0;

void expect_true(bool cond, const char* msg) {
    if (!cond) {
        std::fprintf(stderr, "FAIL: %s\n", msg);
        ++g_failures;
    } else {
        std::printf("ok: %s\n", msg);
    }
}

void test_local_hit_no_redis() {
    LocalUrlCache cache;
    cache.configure(4, 1024 * 1024, 64 * 1024, 3600, 0, 30);
    cache.put_positive("abc123", "https://example.com/x", "");

    LocalUrlCache::EntryView view;
    const LocalUrlCache::Status st = cache.get("abc123", &view);
    expect_true(st == LocalUrlCache::Status::Hit, "local positive hit");
    expect_true(view.long_url == "https://example.com/x", "local hit returns url");
    expect_true(view.expire_at.empty(), "empty expire_at means never");
}

void test_local_business_expired_is_410_not_hit() {
    LocalUrlCache cache;
    cache.configure(4, 1024 * 1024, 64 * 1024, 3600, 0, 30);
    cache.put_positive("exp1", "https://example.com/old", "2000-01-01 00:00:00");

    LocalUrlCache::EntryView view;
    const LocalUrlCache::Status st = cache.get("exp1", &view);
    expect_true(st == LocalUrlCache::Status::Expired,
                "business-expired cached mapping is Expired (410), not Hit");
    expect_true(view.long_url == "https://example.com/old", "expired still exposes url");

    // Dropped from positive; next get is Miss (refill), not a stale 302 Hit.
    LocalUrlCache::EntryView view2;
    const LocalUrlCache::Status st2 = cache.get("exp1", &view2);
    expect_true(st2 == LocalUrlCache::Status::Miss,
                "after business expiry drop, cache misses for refill");
}

void test_cache_ttl_expiry_is_miss_not_410() {
    LocalUrlCache cache;
    cache.configure(2, 1024 * 1024, 64 * 1024, /*ttl*/1, 0, 30);
    cache.put_positive("ttl1", "https://example.com/ttl", "");

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    LocalUrlCache::EntryView view;
    const LocalUrlCache::Status st = cache.get("ttl1", &view);
    expect_true(st == LocalUrlCache::Status::Miss,
                "cache TTL expiry is Miss (refill), not business Expired");
}

void test_negative_budget_separate() {
    LocalUrlCache cache;
    // Tiny positive budget, larger negative — random neg inserts must not wipe positive.
    cache.configure(1, /*pos*/200, /*neg*/1024 * 1024, 3600, 0, 30);
    cache.put_positive("hot", "https://example.com/hot", "");

    for (int i = 0; i < 200; ++i) {
        cache.put_negative("n" + std::to_string(i));
    }

    LocalUrlCache::EntryView view;
    const LocalUrlCache::Status st = cache.get("hot", &view);
    expect_true(st == LocalUrlCache::Status::Hit,
                "negative inserts cannot flush hot positive (separate budgets)");
}

void test_singleflight_shares_one_lookup() {
    SingleFlight sf;
    sf.configure(64, 64);

    std::atomic<int> origin_calls(0);
    std::atomic<int> hits(0);
    const int n = 32;
    std::vector<std::thread> threads;
    threads.reserve(n);

    for (int i = 0; i < n; ++i) {
        threads.emplace_back([&]() {
            SingleFlightResult r = sf.do_flight("hotcode", [&]() {
                origin_calls.fetch_add(1);
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                SingleFlightResult out;
                out.kind = SingleFlightResult::Kind::Hit;
                out.long_url = "https://example.com/shared";
                out.expire_at = "";
                return out;
            });
            if (r.kind == SingleFlightResult::Kind::Hit &&
                r.long_url == "https://example.com/shared") {
                hits.fetch_add(1);
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }

    expect_true(origin_calls.load() == 1,
                "concurrent misses share one origin lookup");
    expect_true(hits.load() == n, "all waiters observe the shared Hit");
}

void test_singleflight_overflow_503() {
    SingleFlight sf;
    sf.configure(/*max_inflight*/1, /*max_waiters*/1);

    std::atomic<bool> loader_entered(false);
    std::atomic<bool> release_loader(false);

    std::thread leader([&]() {
        sf.do_flight("k", [&]() {
            loader_entered.store(true);
            while (!release_loader.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
            }
            SingleFlightResult out;
            out.kind = SingleFlightResult::Kind::Hit;
            out.long_url = "https://example.com/a";
            return out;
        });
    });

    while (!loader_entered.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    // One waiter allowed.
    std::thread waiter([&]() {
        SingleFlightResult r = sf.do_flight("k", []() {
            SingleFlightResult out;
            out.kind = SingleFlightResult::Kind::Error;
            out.error = "should not run";
            return out;
        });
        expect_true(r.kind == SingleFlightResult::Kind::Hit ||
                        r.kind == SingleFlightResult::Kind::Overload,
                    "waiter gets shared result or overload");
    });

    // Second waiter / second key should overload when inflight is full.
    SingleFlightResult other = sf.do_flight("other", []() {
        SingleFlightResult out;
        out.kind = SingleFlightResult::Kind::Hit;
        return out;
    });
    expect_true(other.kind == SingleFlightResult::Kind::Overload,
                "inflight overflow returns Overload (503)");

    release_loader.store(true);
    leader.join();
    waiter.join();
}

void test_bloom_default_not_hard_404() {
    Config cfg;
    expect_true(cfg.bloom_hard_filter == false,
                "bloom_hard_filter defaults to false");

    BloomFilter bloom;
    bloom.reset(1024, 4);
    bloom.add("known");
    expect_true(bloom.might_contain("known"), "bloom contains added code");
    expect_true(!bloom.might_contain("brand-new-code-xyz"),
                "unknown code is a bloom miss");

    // Soft policy: bloom miss must not be treated as authoritative 404.
    // Handler only 404s on bloom miss when bloom_hard_filter is true.
    const bool would_hard_404 = cfg.bloom_hard_filter &&
                                !bloom.might_contain("brand-new-code-xyz");
    expect_true(!would_hard_404,
                "default bloom policy does not hard-404 unknown-to-bloom codes");
}

void test_is_business_expired_helper() {
    expect_true(!LocalUrlCache::is_business_expired(""),
                "empty expire_at is never expired");
    expect_true(LocalUrlCache::is_business_expired("2000-01-01 00:00:00"),
                "past expire_at is expired");
    expect_true(!LocalUrlCache::is_business_expired("2099-01-01 00:00:00"),
                "future expire_at is not expired");
}

void test_legacy_redis_payload_is_miss_not_never_expire_hit() {
    // Old bug: plain URL → Hit with empty expire_at → put_local_positive(..., "")
    // → L1 302 until cache TTL even when MySQL expire_at is past.
    ShortUrlCache::LookupValue value;
    value.long_url = "should-not-matter";
    value.expire_at = "should-not-matter";

    const ShortUrlCache::CacheStatus plain =
        ShortUrlCache::interpret_redis_payload("https://example.com/legacy", &value);
    expect_true(plain == ShortUrlCache::CacheStatus::Miss,
                "plain Redis URL is Miss (refill MySQL), not Hit");
    expect_true(!ShortUrlCache::decode_redis_value("https://example.com/legacy",
                                                   nullptr, nullptr),
                "plain URL fails v1 decode");

    const ShortUrlCache::CacheStatus garbage =
        ShortUrlCache::interpret_redis_payload("v1-no-newlines", &value);
    expect_true(garbage == ShortUrlCache::CacheStatus::Miss,
                "undecodable Redis payload is Miss");

    const ShortUrlCache::CacheStatus truncated =
        ShortUrlCache::interpret_redis_payload("v1\nonly-expire", &value);
    expect_true(truncated == ShortUrlCache::CacheStatus::Miss,
                "truncated v1 payload is Miss");

    // Valid v1 never-expire remains Hit; must not regress that path.
    const std::string encoded =
        ShortUrlCache::encode_redis_value("https://example.com/ok", "");
    ShortUrlCache::LookupValue hit_val;
    const ShortUrlCache::CacheStatus hit =
        ShortUrlCache::interpret_redis_payload(encoded, &hit_val);
    expect_true(hit == ShortUrlCache::CacheStatus::Hit, "encoded v1 empty expire is Hit");
    expect_true(hit_val.long_url == "https://example.com/ok", "v1 hit returns url");
    expect_true(hit_val.expire_at.empty(), "v1 empty expire_at means never");

    // Encoded past expire_at stays Expired (410), never Hit.
    const std::string expired_raw =
        ShortUrlCache::encode_redis_value("https://example.com/old",
                                          "2000-01-01 00:00:00");
    ShortUrlCache::LookupValue exp_val;
    const ShortUrlCache::CacheStatus expired =
        ShortUrlCache::interpret_redis_payload(expired_raw, &exp_val);
    expect_true(expired == ShortUrlCache::CacheStatus::Expired,
                "encoded v1 past expire_at is Expired not Hit");
}

} // namespace

int main() {
    test_local_hit_no_redis();
    test_local_business_expired_is_410_not_hit();
    test_cache_ttl_expiry_is_miss_not_410();
    test_negative_budget_separate();
    test_singleflight_shares_one_lookup();
    test_singleflight_overflow_503();
    test_bloom_default_not_hard_404();
    test_is_business_expired_helper();
    test_legacy_redis_payload_is_miss_not_never_expire_hit();

    if (g_failures != 0) {
        std::fprintf(stderr, "%d failure(s)\n", g_failures);
        return 1;
    }
    std::printf("all local-cache tests passed\n");
    return 0;
}
