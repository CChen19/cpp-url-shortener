# Phase 0 baseline harness

Honest baseline for judging later optimizations. **This document does not claim a QPS number.** Result tables stay empty until a run on a frozen machine fills them with measured values.

Canonical assumptions: [business_assumptions.md](business_assumptions.md).

## Freeze checklist (record before any numbered result)

| Knob | Recorded value | Notes |
|------|----------------|-------|
| Host / CPU model / cores | _empty_ | Prefer bare metal or fixed WSL2 VM size |
| OS / kernel | _empty_ | |
| Compiler / build type | _empty_ | **Do not** treat ASan/TSan/UBSan builds as perf |
| Binary path / git SHA | _empty_ | |
| `config/config.yaml` (or overlay) | _empty_ | Port, `thread_num`, `actor_model`, Redis/Kafka flags |
| Log knobs | _empty_ | Prefer sync off or async on consistently; note path |
| MySQL version / pool_size / sharding | _empty_ | |
| Redis version / TTL / jitter | _empty_ | |
| Kafka brokers / topic / enabled | _empty_ | |
| Driver | fixed-rate Python (primary) | webbench/wrk closed-loop = secondary only |
| Arrival rate / duration / warmup | _empty_ | |

## Primary vs secondary load

- **Primary:** fixed arrival rate (open-loop). Default driver: `test_pressure/drivers/fixed_rate_http.py`. Also acceptable: vegeta or wrk2 if installed.
- **Secondary:** webbench / wrk closed-loop concurrency sweeps. Useful for historical comparison only. **webbench `pages/min` is not QPS** (see [phase1_baseline.md](phase1_baseline.md)).

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

## Result tables (empty until measured)

### Scenario 1 — hot

| rate (req/s) | duration | 302 | 201 | 404 | 410 | 503 | other | timeout% | error% | P50 ms | P95 ms | P99 ms | CPU% | RSS MB | ctxt/s |
|--------------|----------|-----|-----|-----|-----|-----|-------|----------|--------|--------|--------|--------|------|--------|--------|
| | | | | | | | | | | | | | | | |

### Scenario 2 — uniform

| rate | duration | 302 | 404 | 410 | 503 | timeout% | error% | P50 | P95 | P99 | notes |
|------|----------|-----|-----|-----|-----|----------|--------|-----|-----|-----|-------|
| | | | | | | | | | | | |

### Scenario 3 — zipf

| rate | zipf_s | codes | 302 | timeout% | P50 | P95 | P99 | notes |
|------|--------|-------|-----|----------|-----|-----|-----|-------|
| | | | | | | | | |

### Scenario 4 — herd

| setup | rate | duration | 302 | 503 | P95 | P99 | notes |
|-------|------|----------|-----|-----|-----|-----|-------|
| | | | | | | | |

### Scenario 5 — negative

| mix | rate | 404 | 410 | 302 (must be ~0 for expired) | P95 | notes |
|-----|------|-----|-----|------------------------------|-----|-------|
| | | | | | | |

### Scenario 6 — mixed

| create_rate | redirect_rate | 201 | 302 | 503 | P95 create | P95 redirect | notes |
|-------------|---------------|-----|-----|-----|------------|--------------|-------|
| | | | | | | | |

### Scenario 7 — deps down

| mode | redirect OK? | create OK? | observed drop/metric | notes |
|------|--------------|------------|----------------------|-------|
| redis disabled | | | | |
| kafka disabled | | | | |
| redis process stopped | | | | |
| kafka process stopped | | | | |

## Historical note

Older `docs/phase1_baseline.md` `/health` rows used webbench and labeled **pages/min** as QPS. Those rows are kept for history; do not treat them as Phase 0 fixed-rate baseline.
