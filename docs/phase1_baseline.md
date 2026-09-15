# Phase 1 Benchmark Baseline

测试目标：保留 Phase 0 的 `/health` 基线，并为短链读写接口留下固定压测入口。后续 Phase 2/3 只追加新数据，不覆盖历史数据。

> **Labeling correction (Phase 0):** The historical webbench column below was titled `QPS (pages/min)`. That is **wrong**. Webbench reports **pages/min**, which is **not** requests/second. Divide by 60 only if you need an approximate RPS from that tool — and still treat it as closed-loop secondary evidence. Authoritative Phase 0 method: fixed arrival rate in [phase0_baseline.md](phase0_baseline.md). Assumptions: [business_assumptions.md](business_assumptions.md).

## 环境

| 项目 | 值 |
|------|----|
| OS | WSL2 / Ubuntu 20.04 |
| Compiler | GCC 9.4.0 |
| MySQL | 8.0.42 |
| Server Port | 9006 |
| Thread Count | 8 |
| Actor Model | Proactor |

## 准备数据

```bash
mysql -h127.0.0.1 -ushorturl -pshorturl shorturl < sql/001_short_url.sql

curl -sS -X POST http://127.0.0.1:9006/api/shorten \
  -H 'Content-Type: application/json' \
  -d '{"long_url":"https://example.com/phase1-baseline"}'
```

记录返回的 `short_code`，替换下面命令里的 `<code>`。

## Phase 0: GET /health

工具：webbench，持续 10 秒。

**Historical table kept as-is.** Column header is intentionally left showing the original mislabel; values are webbench **pages/min**, not QPS.

| 并发数 | pages/min (was mislabeled "QPS") | 每秒吞吐量 | 成功请求 | 失败 |
|--------|----------------------------------|------------|----------|------|
| 500    | 287,922                          | 585 KB/s   | 47,987   | 0    |
| 1,000  | 292,638                          | 595 KB/s   | 48,773   | 0    |
| 5,000  | 309,660                          | 630 KB/s   | 51,610   | 0    |

## Phase 1: GET /{code}

```bash
test_pressure/webbench-1.5/webbench -c 500 -t 10 http://127.0.0.1:9006/<code>
test_pressure/webbench-1.5/webbench -c 1000 -t 10 http://127.0.0.1:9006/<code>
test_pressure/webbench-1.5/webbench -c 5000 -t 10 http://127.0.0.1:9006/<code>
```

Prefer Phase 0 fixed-rate scenarios for new numbers:

```bash
./test_pressure/run_scenarios.sh --scenario hot --rate 200 --duration 15s
```

| 并发数 | pages/min (was mislabeled "QPS") | 每秒吞吐量 | 成功请求 | 失败 | 备注 |
|--------|----------------------------------|------------|----------|------|------|
| 500    | TBD                              | TBD        | TBD      | TBD  | 302 + MySQL read |
| 1,000  | TBD                              | TBD        | TBD      | TBD  | 302 + MySQL read |
| 5,000  | TBD                              | TBD        | TBD      | TBD  | 302 + MySQL read |

## Phase 1: POST /api/shorten

`webbench` 只适合 GET 基准；POST 写入建议用 Phase 0 fixed-rate driver，或次选 `wrk` / `hey`。

```bash
# Primary (fixed rate)
python3 test_pressure/drivers/fixed_rate_http.py \
  --rate 50 --duration 10s --method POST \
  --url http://127.0.0.1:9006/api/shorten \
  --body '{"long_url":"https://example.com/phase1-post-baseline"}' \
  --header 'Content-Type: application/json'

# Secondary (closed-loop)
wrk -t8 -c500 -d10s -s test_pressure/shorten.lua http://127.0.0.1:9006
```

| 并发数 | req/s (TBD; not pages/min) | 平均延迟 | P95 | 成功请求 | 失败 | 备注 |
|--------|----------------------------|----------|-----|----------|------|------|
| 500    | TBD                        | TBD      | TBD | TBD      | TBD  | JSON parse + Snowflake + MySQL insert |
