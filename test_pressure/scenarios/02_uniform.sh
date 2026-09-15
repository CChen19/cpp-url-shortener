#!/usr/bin/env bash
# Scenario 2: many codes, uniform mix.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER="$ROOT/test_pressure/drivers/fixed_rate_http.py"
TARGETS="${TARGETS:?set TARGETS to uniform_targets.txt}"
OUT="${OUT:?}"
RATE="${RATE:-200}"
DURATION="${DURATION:-15s}"
WORKERS="${WORKERS:-64}"
TIMEOUT="${TIMEOUT:-2s}"
DRY_RUN="${DRY_RUN:-0}"

cmd=(python3 "$DRIVER" --rate "$RATE" --duration "$DURATION" --targets "$TARGETS"
     --workers "$WORKERS" --timeout "$TIMEOUT" --out "$OUT/uniform.json")
if [[ "$DRY_RUN" == "1" ]]; then
  cmd+=(--dry-run)
fi
echo "+ ${cmd[*]}"
"${cmd[@]}"
