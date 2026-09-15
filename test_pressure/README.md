# Phase 0 short-URL load harness

Primary driver: **fixed arrival rate** (`drivers/fixed_rate_http.py`).  
Secondary only: webbench (GET) and wrk+lua (POST). Do **not** treat webbench `pages/min` as QPS.

Docs: [docs/phase0_baseline.md](../docs/phase0_baseline.md), [docs/business_assumptions.md](../docs/business_assumptions.md).

## Quick start

```bash
# From repo root — dry-run (no server required)
./test_pressure/run_scenarios.sh --dry-run

# List scenarios
./test_pressure/run_scenarios.sh --list

# Live run (server + MySQL required; Redis/Kafka per scenario)
./test_pressure/run_scenarios.sh --rate 200 --duration 15s

# One scenario
./test_pressure/run_scenarios.sh --scenario hot --rate 200 --duration 15s
```

Results: `test_pressure/results/<timestamp>/` (gitignored).

## Scenarios

| Name | What |
|------|------|
| `hot` | Single hot short code |
| `uniform` | Many codes, uniform |
| `zipf` | Zipf hotspot mix |
| `herd` | Flush Redis hot key then slam (thundering herd) |
| `negative` | Missing `404` + business-expired `410` |
| `mixed` | Parallel create + redirect fixed rates |
| `deps` | Redis/Kafka down notes + optional redirect probe |

Overlays: `overlays/bench_redis_disabled.yaml`, `overlays/bench_kafka_disabled.yaml`.

## Host metrics

```bash
./test_pressure/drivers/host_metrics.sh --pid <server_pid> --interval 1 --duration 30 --out /tmp/host.csv
```

## Secondary tools (optional)

```bash
# Build webbench once
make -C test_pressure/webbench-1.5

# Closed-loop GET (pages/min ≠ QPS)
test_pressure/webbench-1.5/webbench -c 500 -t 10 http://127.0.0.1:9006/<code>

# Closed-loop POST
wrk -t8 -c100 -d10s -s test_pressure/shorten.lua http://127.0.0.1:9006
```

## Layout

```text
run_scenarios.sh          entrypoint
configs/common.env        defaults
drivers/fixed_rate_http.py
drivers/host_metrics.sh
tools/seed_codes.py
scenarios/01_*.sh … 07_*.sh
overlays/*.yaml
shorten.lua               wrk POST helper (secondary)
webbench-1.5/             historical GET tool (secondary)
```
