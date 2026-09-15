#!/usr/bin/env bash
# Scenario 5: missing (404) and business-expired (410) codes.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER="$ROOT/test_pressure/drivers/fixed_rate_http.py"
TARGETS="${TARGETS:?set TARGETS to negative_targets.txt}"
OUT="${OUT:?}"
RATE="${RATE:-100}"
DURATION="${DURATION:-10s}"
WORKERS="${WORKERS:-32}"
TIMEOUT="${TIMEOUT:-2s}"
DRY_RUN="${DRY_RUN:-0}"

cmd=(python3 "$DRIVER" --rate "$RATE" --duration "$DURATION" --targets "$TARGETS"
     --workers "$WORKERS" --timeout "$TIMEOUT" --out "$OUT/negative.json")
if [[ "$DRY_RUN" == "1" ]]; then
  cmd+=(--dry-run)
fi
echo "+ ${cmd[*]}"
"${cmd[@]}"
echo "expect: mix of 404 (missing) and 410 (expired); 302 for expired is a FAIL"
