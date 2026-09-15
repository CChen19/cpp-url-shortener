# Phase 2 Redis Cache Consistency

Phase 2 目标：为短链跳转链路加入 Redis 缓存层，并把缓存一致性、穿透、击穿、雪崩作为核心章节沉淀下来。

## 依赖

项目通过 `redis-plus-plus` 接入 Redis。CMake 会自动探测：

- `sw/redis++/redis++.h`
- `libredis++`
- `libhiredis`

如果依赖缺失，项目仍可构建，缓存层以 DB-only fallback 运行；安装依赖后重新 CMake 配置即可启用真实 Redis。

本地安装示例：

```bash
sudo apt install -y redis-server libhiredis-dev

cd /tmp
git clone https://github.com/sewenew/redis-plus-plus.git
cd redis-plus-plus
mkdir -p build && cd build
cmake .. -DREDIS_PLUS_PLUS_CXX_STANDARD=14
make -j
sudo make install
sudo ldconfig
```

## 配置

```yaml
redis:
  enabled: true
  uri: "tcp://127.0.0.1:6379"
  connect_timeout_ms: 200
  socket_timeout_ms: 200

cache:
  ttl_seconds: 3600
  ttl_jitter_seconds: 300
  bloom_bits: 1048576
  bloom_hashes: 7
  bloom_hard_filter: false
  local_shards: 16
  local_positive_bytes: 67108864
  local_negative_bytes: 4194304
  local_negative_ttl_seconds: 30
  singleflight_max_inflight: 1024
  singleflight_max_waiters_per_key: 256
```

Redis key:

```text
shorturl:{code} -> v1\n{expire_at}\n{long_url}
```
## MySQL 连接约定

项目使用专用应用用户：

```yaml
mysql:
  host: "127.0.0.1"
  user: "shorturl"
  password: "shorturl"
  database: "shorturl"
```

显式使用 `127.0.0.1` 是为了走 TCP。Ubuntu/WSL 环境里，MySQL 客户端如果不写 `-h127.0.0.1`，常会默认走 Unix socket：

```text
/var/run/mysqld/mysqld.sock
```

当 socket 目录权限异常或 `root@localhost` 使用 `auth_socket` 时，会出现 `ERROR 2002` 或 `ERROR 1698`。项目运行不依赖 socket，只要 TCP 验证通过即可：

```bash
mysqladmin -h127.0.0.1 -ushorturl -pshorturl ping
mysql -h127.0.0.1 -ushorturl -pshorturl shorturl -e "SHOW TABLES;"
```

## Cache-Aside 路径

### 读路径：GET /{code}

1. Optional Bloom hard-filter (default **off**): only if `bloom_hard_filter: true` and bloom is ready and miss → 404. Soft/default: bloom miss is a hint; continue.
2. L1 local cache (sharded, byte-budgeted): values carry `long_url` + business `expire_at` + cache-expire time.
3. L1 Hit and not business-expired → 302 (no Redis/MySQL).
4. L1 Hit but past `expire_at` → **410**, drop entry. Never 302. Cache TTL expiry → Miss (refill), not 410.
5. L1 negative hit → 404.
6. L1 Miss → singleflight: one shared origin lookup per code (waiters share Hit / NotFound / Expired / Error). Overflow → 503.
7. Origin: Redis L2 (value encodes `expire_at`) → on miss/unavailable, MySQL.
8. Redis/MySQL Hit: populate L1 (+ Redis if up) with `expire_at`, return 302.
9. Business-expired at Redis/MySQL → erase caches, return **410** (never 302).
10. MySQL NotFound → short negative L1, return 404.

### 写路径：POST /api/shorten

1. Generate Snowflake ID → Base62 `short_code`.
2. Write MySQL.
3. On success: always write L1 and (if Redis up) Redis with **url + expire_at** (empty expire_at = never). Cache TTL ≠ business expiry.

Redis value encoding:

```text
shorturl:{code} -> "v1\n{expire_at}\n{long_url}"
```

Legacy plain-URL values are still readable and treated as never-expiring.

## 三防

### 穿透：Bloom Filter（默认软）

Bloom warmup may still populate from unexpired codes. It is **not** the final authority.

- Default `bloom_hard_filter: false`: bloom miss does not hard-404; unknown-to-bloom codes still reach L1/Redis/MySQL.
- Opt-in `bloom_hard_filter: true` is **single-process-only**. Integrity is not guaranteed if another writer exists. Also: codes that were unexpired at warmup and later expire can 404 vs 410 until restart.

Negative L1 (short TTL, separate byte budget) limits random-code pressure on hot positives.

### 击穿：Singleflight（共享结果）

```text
singleflight key = short_code
```

One in-flight origin loader per key. Waiters block on a condvar and **share** the loader result (including Redis-down DB path). Bounds: `singleflight_max_inflight` / `singleflight_max_waiters_per_key` → 503 on overflow. Waiters do not hold a MySQL connection while waiting.

### 雪崩：TTL 随机抖动

Redis / L1 positive writes use:

```text
ttl = ttl_seconds + random(0, ttl_jitter_seconds)
```

### 业务过期 vs 缓存 TTL

- Cache TTL controls how long a value may stay in L1/Redis.
- `expire_at` controls whether the mapping is still valid.
- A still-cached but business-expired mapping must return **410**, never a stale **302**.
- A cache miss means refill from authoritative storage, not "not found".

## 一致性策略

### 当前阶段：Cache-Aside 足够

短链核心语义是“创建后几乎不可变”：

- `short_code -> long_url` 创建后不更新。
- 读多写少。
- 创建接口以 MySQL 写成功为准。
- Redis 只是加速层，miss 可回源。

因此 Phase 2 使用 Cache-Aside 就足够：写 DB 后更新 BF 并尝试写缓存；读 miss 时回源 DB 再重建缓存。

### 禁用/过期场景：延时双删

如果后续加入禁用短链、修改过期时间、人工封禁等写路径，需要讨论延时双删：

1. 先删除 Redis。
2. 更新 MySQL。
3. 延迟几十到几百毫秒后再次删除 Redis。

原因：并发读可能在第一次删除后、DB 更新前读到旧值并重建缓存；第二次删除用于清掉这个窗口期产生的脏缓存。

对于短链禁用，推荐 DB 增加 `disabled_at` 或 `status` 字段，并在查询条件里统一过滤。

### Bonus：Canal 订阅 binlog

如果面试继续追问更强一致性或多服务写入：

- MySQL binlog 作为变更源。
- Canal 订阅 `short_url` 表变更。
- 消费变更事件异步删除或刷新 Redis。
- 与延时双删相比，Canal 更适合多写入口、多服务、多语言系统。

这个方案本质是用 CDC 把 DB 变更广播给缓存维护组件，降低业务代码里到处写删缓存逻辑的耦合。

## 当前代码落点

| 模块 | 说明 |
|------|------|
| `shorturl/short_url_cache.{h,cpp}` | Redis Cache-Aside、TTL jitter、缓存降级 |
| `shorturl/bloom_filter.{h,cpp}` | 本地 Bloom Filter |
| `shorturl/singleflight.{h,cpp}` | 按 code 的互斥重建 |
| `shorturl/short_url_repository.{h,cpp}` | 增加合法 code 预热查询 |
| `handler/short_url_handler.cpp` | 接入读写缓存路径 |

## 验证记录

本地端到端验证结果：

```bash
redis-cli ping
# PONG

mysqladmin -h127.0.0.1 -ushorturl -pshorturl ping
# mysqld is alive

mysql -h127.0.0.1 -ushorturl -pshorturl shorturl -e "SHOW TABLES;"
# short_url
```

创建短链：

```bash
curl -sS -X POST http://127.0.0.1:9006/api/shorten \
  -H 'Content-Type: application/json' \
  -d '{"long_url":"https://example.com/phase2-test"}'
```

示例响应：

```json
{
  "expire_at": null,
  "long_url": "https://example.com/phase2-test",
  "short_code": "KuBmfe",
  "short_url": "http://127.0.0.1:9006/KuBmfe"
}
```

缓存验证：

```bash
redis-cli get shorturl:KuBmfe
# https://example.com/phase2-test

redis-cli ttl shorturl:KuBmfe
# 3806
```

`3806 = 3600 + jitter`，说明 TTL 随机抖动已生效。

跳转验证：

```bash
curl -i http://127.0.0.1:9006/KuBmfe
# HTTP/1.1 302 Found
# Location: https://example.com/phase2-test
```
