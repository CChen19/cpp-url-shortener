# Phase 0 baseline harness

Honest baseline for judging later optimizations. **This document does not claim a QPS number.**

Two filled passes exist on one **Windows gaming laptop / WSL2 stand-in**, not a
server. See [laptop_wsl2_experiment.md](laptop_wsl2_experiment.md). The freeze
table and the numeric tables below are **Pass 1 (Kafka down)** unless a row or
note says Kafka-on. They are not capacity ratings.

Canonical assumptions: [business_assumptions.md](business_assumptions.md).

## Freeze checklist (record before any numbered result)

| Knob | Recorded value | Notes |
|------|----------------|-------|
| Host / CPU model / cores | Legion R9000P 2021H, Ryzen 7 5800H, 8c/16t, 11 GiB WSL2 | **Laptop stand-in, not a server** |
| OS / kernel | Ubuntu 20.04.3 LTS / `6.18.33.2-microsoft-standard-WSL2` | |
| Compiler / build type | g++ 9.4.0, Release | **Do not** treat ASan/TSan/UBSan builds as perf |
| Binary path / git SHA | `./build-linux/server`; Pass 1 = `7865706` + Release `addsig`; Pass 2 also Kafka worker `poll` | See laptop experiment |
| `config/config.yaml` (or overlay) | port 9006, `thread_num=8`, `actor_model=0` | Pass 1: Redis on, Kafka enabled but broker down |
| Log knobs | `log.async=false`; structured JSONL on | `./logs/` |
| MySQL version / pool_size / sharding | 8.0.42 / 8 / 4×4 present | `127.0.0.1:3306` |
| Redis version / TTL / jitter | 5.0.7 / 3600s / 300s | `127.0.0.1:6379` |
| Kafka brokers / topic / enabled | `127.0.0.1:9092` / `shorturl.clicks` / enabled | Pass 1: **not listening**. Pass 2: native 3.7.2 KRaft |
| Driver | fixed-rate Python (primary) | webbench/wrk closed-loop = secondary only |
| Arrival rate / duration / warmup | 200 req/s × 15s; extra hot 500×15s | Seed 50 codes |

## Primary vs secondary load

- **Primary:** fixed arrival rate (open-loop). Default driver: `test_pressure/drivers/fixed_rate_http.py`. Also acceptable: vegeta or wrk2 if installed.
- The driver **does not follow `Location`**; first-hop statuses (`302`/`201`/`404`/`410`/`503`) are recorded as-is.
- Under overload, if the scheduler falls behind the target interval it **skips forward** (does not burst catch-up traffic).
- `timeout_rate` / `error_rate` use **completed** attempts as the denominator; the JSON also reports `*_of_requested`.
- **Secondary:** webbench / wrk closed-loop concurrency sweeps. Useful for historical comparison only. **webbench `pages/min` is not QPS** (see [phase1_baseline.md](phase1_baseline.md)).

Self-check (no server deps):

```bash
python3 test_pressure/tools/test_no_redirect.py
```

## How to run

From repo root, with server listening (default `127.0.0.1:9006`):

```bash
# Dry-run: print commands, no traffic
./test_pressure/run_scenarios.sh --dry-run

# Seed codes then run all scenarios (skips deps that are down)
./test_pressure/run_scenarios.sh --rate 200 --duration 15s

# One scenario
./test_pressure/run_scenarios.sh --scenario hot --rate 200 --duration 15s
```

Outputs land under `test_pressure/results/<timestamp>/` (gitignored).

### Host metrics during a run

```bash
./test_pressure/drivers/host_metrics.sh --pid "$(pgrep -n -f './build-linux/server')" --interval 1 --out /tmp/host_metrics.csv
```

Captures CPU%, RSS, and voluntary/involuntary context switches from `/proc`.

### Later profiling hooks (not a full profile yet)

```bash
# CPU on-CPU samples (requires perf)
perf record -F 99 -p <pid> -g -- sleep 30

# Off-CPU / blocking sketch (requires bpftrace root)
# bpftrace -e 'kprobe:schedule { @[comm]=count(); }'
```

Placeholders for server-side counters (fill when metrics exist): queue depth, queue wait, DB query count, allocs, lock wait. Today scrape `GET /metrics` if the binary exports them.

## Scenarios

| ID | Name | Intent | Script path |
|----|------|--------|-------------|
| 1 | `hot` | Single hot short code | `test_pressure/scenarios/01_hot.sh` |
| 2 | `uniform` | Many codes, uniform | `test_pressure/scenarios/02_uniform.sh` |
| 3 | `zipf` | Zipf hotspot mix | `test_pressure/scenarios/03_zipf.sh` |
| 4 | `herd` | Cold cache / simultaneous hotspot refill | `test_pressure/scenarios/04_herd.sh` |
| 5 | `negative` | Missing + business-expired codes | `test_pressure/scenarios/05_negative.sh` |
| 6 | `mixed` | Mixed create + redirect | `test_pressure/scenarios/06_mixed.sh` |
| 7 | `deps` | Redis down / Kafka down degrade hooks | `test_pressure/scenarios/07_deps_down.sh` |

### Expected status classes (current code)

| Outcome | HTTP | Notes |
|---------|------|-------|
| Redirect | 302 | Hit or DB refill of active mapping |
| Create | 201 | `POST /api/shorten` success |
| Missing | 404 | Unknown code (or Bloom filter reject) |
| Business-expired | 410 | `expire_at <= NOW()` |
| Storage unavailable | 503 | MySQL create/find failure |

### Expected degrade behavior today (document, do not invent numbers)

- **Redis down / disabled:** cache miss path falls through to MySQL; redirect still served if DB has the row. Overlay: `test_pressure/overlays/bench_redis_disabled.yaml`.
- **Kafka down / disabled:** `publish_click` may fail; redirect still returns 302 because publish success is not required for the response. Overlay: `test_pressure/overlays/bench_kafka_disabled.yaml`. Drops should be visible via Kafka publish metrics when observability is enabled.

## Result tables (laptop / WSL2 stand-in, 2026-09-14)

Source: [laptop_wsl2_experiment.md](laptop_wsl2_experiment.md). CPU%/RSS omitted
(`host_metrics.sh` sample was not credible). Not a server QPS.

### Scenario 1 — hot

| rate (req/s) | duration | 302 | 201 | 404 | 410 | 503 | other | timeout% | error% | P50 ms | P95 ms | P99 ms | CPU% | RSS MB | ctxt/s |
|--------------|----------|-----|-----|-----|-----|-----|-------|----------|--------|--------|--------|--------|------|--------|--------|
| 200 | 15s | 3000 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.728 | 0.924 | 1.075 | — | — | — |
| 500 (Kafka down) | 15s | 7406 | 0 | 0 | 0 | 0 | 1 timeout | 0.0135 | 0 | 0.694 | 2.738 | 79.938 | — | — | — |
| 500 (Kafka on) | 15s | 7500 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0.609 | 0.763 | 0.873 | — | — | — |

Pass 1 500/s Kafka-down: completed 7407 vs ~7500 scheduled; 302 throughput 493.7/s.

**Kafka-on follow-up (same laptop, native Kafka 3.7.2 + consumer):** see
[laptop_wsl2_experiment.md](laptop_wsl2_experiment.md#full-stack-follow-up-kafka-on--same-laptop-2026-09-14).
200/s tables look the same (~1 ms P99). Hot **500/s held 7500×302**, P99 0.873 ms.
Enqueue drops 0; consumer lagged (not on the 302 path).

### Scenario 2 — uniform

| rate | duration | 302 | 404 | 410 | 503 | timeout% | error% | P50 | P95 | P99 | notes |
|------|----------|-----|-----|-----|-----|----------|--------|-----|-----|-----|-------|
| 200 | 15s | 3000 | 0 | 0 | 0 | 0 | 0 | 0.771 | 1.006 | 1.248 | 50 seeded codes |

### Scenario 3 — zipf

| rate | zipf_s | codes | 302 | timeout% | P50 | P95 | P99 | notes |
|------|--------|-------|-----|----------|-----|-----|-----|-------|
| 200 | 1.2 | 50 | 3000 | 0 | 0.704 | 0.920 | 1.047 | |

### Scenario 4 — herd

| setup | rate | duration | 302 | 503 | P95 | P99 | notes |
|-------|------|----------|-----|-----|-----|-----|-------|
| `DEL` Redis hot key, then slam | 200 | 15s | 3000 | 0 | 0.943 | 1.122 | L1/singleflight; still 302 |

### Scenario 5 — negative

| mix | rate | 404 | 410 | 302 (must be ~0 for expired) | P95 | notes |
|-----|------|-----|-----|------------------------------|-----|-------|
| missing + expired | 200 | 1474 | 1526 | **0** | 1.551 | expired stayed 410 |

### Scenario 6 — mixed

| create_rate | redirect_rate | 201 | 302 | 503 | P95 create | P95 redirect | notes |
|-------------|---------------|-----|-----|-----|------------|--------------|-------|
| 20 | 180 | 300 | 2700 | 0 | 5.846 | 0.847 | 15s |

### Scenario 7 — deps down

| mode | redirect OK? | create OK? | observed drop/metric | notes |
|------|--------------|------------|----------------------|-------|
| redis disabled | not run | | | overlay not used this day |
| kafka disabled | not run | | | overlay not used this day |
| redis process stopped | not run | | | Redis was up |
| kafka process stopped | **yes** (3000×302 @ 200/s) | seed/create used MySQL | enqueue accepted; delivery failure 23843 | broker never started |

## Historical note

Older `docs/phase1_baseline.md` `/health` rows used webbench and labeled **pages/min** as QPS. Those rows are kept for history; do not treat them as Phase 0 fixed-rate baseline.
