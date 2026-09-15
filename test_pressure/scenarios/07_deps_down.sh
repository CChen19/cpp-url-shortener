#!/usr/bin/env bash
# Scenario 7: Redis/Kafka down degrade hooks.
# Documents expected current behavior and runs redirect traffic when possible.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER="$ROOT/test_pressure/drivers/fixed_rate_http.py"
BASE_URL="${BASE_URL:-http://127.0.0.1:9006}"
TARGETS="${TARGETS:-}"
OUT="${OUT:?}"
RATE="${RATE:-100}"
DURATION="${DURATION:-10s}"
WORKERS="${WORKERS:-32}"
TIMEOUT="${TIMEOUT:-2s}"
DRY_RUN="${DRY_RUN:-0}"

mkdir -p "$OUT"
report="$OUT/deps_down_notes.md"

cat >"$report" <<'EOF'
# Deps-down expected behavior (current code)

## Redis

- Config knob: `redis.enabled: false` via `test_pressure/overlays/bench_redis_disabled.yaml`
- Or stop Redis process while leaving `redis.enabled: true` (connect/ping fails → `redis_available_=false`)
- Expected: redirect still served from MySQL on miss; Bloom/cache hits unavailable
- Hook: compare `GET /metrics` cache hit counters before/after; host_metrics during run

## Kafka

- Config knob: `kafka.enabled: false` via `test_pressure/overlays/bench_kafka_disabled.yaml`
- Or stop Kafka brokers while enabled
- Expected: `publish_click` may fail; **redirect still returns 302** (publish result ignored on hot path)
- Hook: `shorturl_kafka_publish_total` failure increments; access logs may show errors on stderr

## Recovery

1. Restore Redis/Kafka
2. Restart server **or** wait for reconnect behavior (today Redis marks unavailable until process restart/init path)
3. Re-run `hot` scenario; confirm 302 and publish success metrics recover

## Manual steps this script does not automate

```bash
# Terminal A — Redis disabled overlay
./build-linux/server test_pressure/overlays/bench_redis_disabled.yaml

# Terminal B — fixed-rate redirect
./test_pressure/run_scenarios.sh --scenario hot --rate 100 --duration 10s

# Terminal A — Kafka disabled overlay
./build-linux/server test_pressure/overlays/bench_kafka_disabled.yaml
```
EOF

echo "wrote $report"

if [[ "$DRY_RUN" == "1" ]]; then
  echo "+ (dry-run) would probe Redis/Kafka and optionally run fixed-rate against TARGETS"
  python3 "$DRIVER" --rate "$RATE" --duration "$DURATION" --url "${BASE_URL}/health" --dry-run
  exit 0
fi

{
  echo
  echo "## Probe at $(date -Is)"
  if command -v redis-cli >/dev/null 2>&1; then
    if redis-cli ping 2>&1; then
      echo "Redis: up"
    else
      echo "Redis: down or unreachable"
    fi
  else
    echo "Redis: redis-cli not installed"
  fi
  if command -v ss >/dev/null 2>&1; then
    if ss -ltn | grep -q ':9092'; then
      echo "Kafka port 9092: listening"
    else
      echo "Kafka port 9092: not listening"
    fi
  fi
  if curl -fsS -m 2 "${BASE_URL}/health" >/dev/null 2>&1; then
    echo "Server health: ok"
  else
    echo "Server health: unreachable"
  fi
} >>"$report"

if [[ -n "$TARGETS" && -f "$TARGETS" ]] && curl -fsS -m 2 "${BASE_URL}/health" >/dev/null 2>&1; then
  python3 "$DRIVER" --rate "$RATE" --duration "$DURATION" --targets "$TARGETS" \
    --workers "$WORKERS" --timeout "$TIMEOUT" --out "$OUT/deps_redirect.json"
else
  echo "skip live deps redirect run (need server + TARGETS); notes still written"
fi
