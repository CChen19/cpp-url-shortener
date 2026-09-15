#!/usr/bin/env bash
# Scenario 4: cold cache / thundering herd on one hotspot.
# Prep: flush Redis key for the hot code (or restart with empty Redis), then slam.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER="$ROOT/test_pressure/drivers/fixed_rate_http.py"
TARGETS="${TARGETS:?}"
OUT="${OUT:?}"
RATE="${RATE:-500}"
DURATION="${DURATION:-10s}"
WORKERS="${WORKERS:-128}"
TIMEOUT="${TIMEOUT:-2s}"
DRY_RUN="${DRY_RUN:-0}"
HOT_CODE="${HOT_CODE:-}"

if [[ "$DRY_RUN" != "1" && -n "$HOT_CODE" ]] && command -v redis-cli >/dev/null 2>&1; then
  if redis-cli ping >/dev/null 2>&1; then
    echo "+ redis-cli DEL shorturl:${HOT_CODE}"
    redis-cli DEL "shorturl:${HOT_CODE}" >/dev/null || true
  else
    echo "note: redis-cli present but Redis not reachable; herd run uses cold-miss if cache empty" >&2
  fi
fi

cmd=(python3 "$DRIVER" --rate "$RATE" --duration "$DURATION" --targets "$TARGETS"
     --workers "$WORKERS" --timeout "$TIMEOUT" --out "$OUT/herd.json")
if [[ "$DRY_RUN" == "1" ]]; then
  cmd+=(--dry-run)
  echo "+ redis-cli DEL shorturl:<hot_code>   # when Redis up"
fi
echo "+ ${cmd[*]}"
"${cmd[@]}"
