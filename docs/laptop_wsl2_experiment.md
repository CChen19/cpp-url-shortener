# Laptop / WSL2 stand-in experiment

**This is not a server result.** It is one honest run on a Windows gaming laptop,
recorded so the Phase 0 tables are no longer empty. Do not treat the numbers as
capacity, SLA, or a QPS claim.

Date: 2026-09-14 (Pacific). Two passes the same day: **Pass 1 Kafka down**,
**Pass 2 MySQL+Redis+Kafka+consumer**. Freeze table below is Pass 1.

## Why run it

The live harness was down for all of Phases 0–4 (`:9006` connection refused).
A filled table on this box is still useful: same driver, same scenarios, same
status classes, with the machine labeled.

## Freeze

| Knob | Recorded value |
|------|----------------|
| Host | Windows gaming laptop (WSL2); no hostname recorded |
| CPU | 8 cores / 16 threads (WSL2 reports 16) |
| Memory (WSL2) | 11 GiB |
| OS | WSL2, Ubuntu 20.04.3 LTS, kernel `6.18.33.2-microsoft-standard-WSL2` |
| Compiler / build | g++ 9.4.0, `CMAKE_BUILD_TYPE=Release` |
| Git | Pass 1: `7865706` + Release `Utils::addsig`. Pass 2: also Kafka worker `poll` |
| Binary | `./build-linux/server` |
| Config | `config/config.yaml`: port 9006, `thread_num=8`, `actor_model=0` |
| Logs | `log.async=false`, structured JSONL enabled, queue 8192 |
| MySQL | 8.0.42 at `127.0.0.1:3306`, user `shorturl`, pool_size 8, 4×4 shards present |
| Redis | 5.0.7 at `127.0.0.1:6379`, enabled, pool_size 8 |
| Kafka (Pass 1) | **not running** (`:9092` not listening). `kafka.enabled=true` in config |
| Driver | `test_pressure/drivers/fixed_rate_http.py` (does not follow `Location`) |
| Rate / duration | 200 req/s × 15s all scenarios; extra hot at 500 req/s × 15s |

WSL `docker` is Docker Desktop’s stub; the Linux engine was not running.
MySQL and Redis: `sudo service mysql start` and `sudo service redis-server start`.

## Release SIGALRM fix (required to measure)

`7865706` built as Release dies after `TIMESLOT` (5s) with `Alarm clock`.
`Utils::addsig` used `assert(sigaction(...))`. Release defines `NDEBUG`, so
the compiler deletes the `assert` **and** the `sigaction` call. `alarm(5)`
then hits `SIG_DFL`.

The binary used for this run calls `sigaction` unconditionally (see
`timer/lst_timer.cpp`). Without that, there is no 15s run. Debug builds of
the same SHA would have installed the handler; tests do not start
`eventListen()`, so ctest did not catch this.

## What the run is

```bash
./test_pressure/run_scenarios.sh --rate 200 --duration 15s
./test_pressure/run_scenarios.sh --scenario hot --rate 500 --duration 15s --skip-seed
```

JSON lands under `test_pressure/results/<timestamp>/` (gitignored).

## Results — 200 req/s × 15s

Driver scheduled 200 arrivals/s. Completed == requested unless noted.
Timeouts and transport errors are 0 except where stated.

| Scenario | rate | 302 | 201 | 404 | 410 | timeout% | P50 ms | P95 ms | P99 ms |
|----------|-----:|----:|----:|----:|----:|---------:|-------:|-------:|-------:|
| hot | 200 | 3000 | 0 | 0 | 0 | 0 | 0.728 | 0.924 | 1.075 |
| uniform | 200 | 3000 | 0 | 0 | 0 | 0 | 0.771 | 1.006 | 1.248 |
| zipf (s=1.2, 50 codes) | 200 | 3000 | 0 | 0 | 0 | 0 | 0.704 | 0.920 | 1.047 |
| herd (Redis hot key deleted first) | 200 | 3000 | 0 | 0 | 0 | 0 | 0.704 | 0.943 | 1.122 |
| negative (missing + expired) | 200 | **0** | 0 | 1474 | 1526 | 0 | 1.090 | 1.551 | 1.747 |
| mixed create | 20 | 0 | 300 | 0 | 0 | 0 | 4.730 | 5.846 | 7.174 |
| mixed redirect | 180 | 2700 | 0 | 0 | 0 | 0 | 0.675 | 0.847 | 1.022 |
| deps (Kafka down, Redis up) | 200 | 3000 | 0 | 0 | 0 | 0 | 0.687 | 0.948 | 1.201 |

Negative: expired codes stayed **410**, never 302. Missing stayed **404**.

## Extra — hot 500 req/s × 15s

| rate | requested | completed | 302 | status 0 / timeout | 302 throughput | P50 | P95 | P99 | max |
|-----:|----------:|----------:|----:|-------------------:|---------------:|----:|----:|----:|----:|
| 500 | 7407 | 7407 | 7406 | 1 | 493.7 /s | 0.694 | 2.738 | 79.938 | 2003 |

The driver fell slightly behind 500/s. One attempt timed out (2s). This is
where this laptop started to show strain; it is not a 500 QPS rating.

## Kafka down (observed)

`publish_click` still **enqueued** (accepted 25107, dropped 0). Background
`producev` accepted 25107. Delivery callback: success 0, **failure 23843**
(broker down; remaining in flight at scrape time). Redirects stayed 302.
That matches “redirect outranks stats; drops/failures must be visible.”

`/metrics` after the run (selected):

- `GET /{code}` 3xx = 25107, 4xx = 3000
- `POST /api/shorten` 2xx = 352
- cache `local_hit` = 25107, `local_neg` = 1473, `redis_miss` = 1526

## Host metrics

`host_metrics.sh` samples were ~0% CPU and a flat ~11 MiB RSS — not credible
next to this load. Likely a pid mismatch or WSL `/proc` accounting.
**CPU% / RSS are omitted from the tables.**

## What the first run did not show

- Bare-metal or cloud server performance
- Kafka-on path (broker was down on that pass)
- Comparison vs pre-optimization `master` (no paired run)
- Saturation / breaking-point QPS
- webbench `pages/min` (not used; not QPS)

## Full-stack follow-up (Kafka on) — same laptop, 2026-09-14

Docker Desktop’s engine was not running. Kafka was a **native** Apache Kafka
**3.7.2** KRaft broker (unpacked tarball, not in this repo), advertised
`127.0.0.1:9092`, topic `shorturl.clicks`. MySQL, Redis, C++ server, and
`python3 consumer/click_consumer.py` (`:9108`) were all up.

The C++ worker had to `rd_kafka_poll` with a short timeout (and poll while
idle). Ubuntu’s librdkafka **1.2.1** does not finish produce I/O on `poll(0)`
only; with Kafka up, deliveries timed out until that worker change. Request
threads still do not poll.

### 200 req/s × 15s (Kafka on)

| Scenario | 302 | 201 | 404 | 410 | timeout% | P50 ms | P95 ms | P99 ms |
|----------|----:|----:|----:|----:|---------:|-------:|-------:|-------:|
| hot | 3000 | 0 | 0 | 0 | 0 | 0.733 | 0.928 | 1.074 |
| uniform | 3000 | 0 | 0 | 0 | 0 | 0.716 | 0.919 | 1.109 |
| zipf | 3000 | 0 | 0 | 0 | 0 | 0.706 | 0.915 | 1.114 |
| herd | 3000 | 0 | 0 | 0 | 0 | 0.690 | 0.865 | 1.016 |
| negative | **0** | 0 | 1474 | 1526 | 0 | 1.144 | 1.644 | 1.888 |
| mixed create 20/s | 0 | 300 | 0 | 0 | 0 | 4.720 | 6.124 | 7.201 |
| mixed redirect 180/s | 2700 | 0 | 0 | 0 | 0 | 0.747 | 0.995 | 1.219 |
| deps (all deps up) | 3000 | 0 | 0 | 0 | 0 | 0.665 | 0.863 | 1.002 |

Redirect P99 stayed ~1 ms vs the Kafka-down pass. Create P99 ~7 ms (MySQL write).

### Extra hot 500 req/s × 15s (Kafka on)

| rate | requested | 302 | timeout | 302/s | P50 | P95 | P99 | max |
|-----:|----------:|----:|--------:|------:|----:|----:|----:|----:|
| 500 | 7500 | 7500 | 0 | 500.0 | 0.609 | 0.763 | 0.873 | 3.429 |

This 500/s pass **held the target**. The earlier Kafka-down 500/s slip is not
repeated here; do not over-read that as “Kafka makes it faster.”

### Click pipeline (after the 200+500 run)

| Counter | Value | Notes |
|---------|------:|-------|
| `kafka_enqueue` accepted / dropped | 25203 / 0 | request path |
| `kafka_produce` accepted / failed | 24340 / 0 | ~15s after run (worker still draining) |
| `kafka_delivery` success / failure | 24339 / 0 | 0 broker failures |
| consumer inserted (immediate) | 14479 | lag 5647 |
| consumer inserted (+15s) | 16553 | lag 7787 (produce still draining) |
| MySQL `click_event` rows | 8 → 8028 then 9192 | includes older leftover rows |
| topic high watermark (+15s) | 24590 | `shorturl.clicks:0` |

The Python consumer commits after each MySQL insert, so it lags the C++
producer under this load. **302s did not wait.** Enqueue drops stayed 0.
Lag is visible on `:9108` (`shorturl_kafka_consumer_lag`).

Seed once hit HTTP 503 (`POST /api/shorten`) while Redis was still settling /
pool busy; two 5xx show on `/metrics` for this process. Seeding was retried
successfully. Not a redirect failure.
