#!/usr/bin/env bash
# Scenario 6: mixed create + redirect at fixed rates.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DRIVER="$ROOT/test_pressure/drivers/fixed_rate_http.py"
BASE_URL="${BASE_URL:-http://127.0.0.1:9006}"
TARGETS="${TARGETS:?redirect targets}"
OUT="${OUT:?}"
CREATE_RATE="${CREATE_RATE:-20}"
REDIRECT_RATE="${REDIRECT_RATE:-180}"
DURATION="${DURATION:-15s}"
WORKERS="${WORKERS:-64}"
TIMEOUT="${TIMEOUT:-2s}"
DRY_RUN="${DRY_RUN:-0}"

create_cmd=(python3 "$DRIVER" --rate "$CREATE_RATE" --duration "$DURATION"
  --method POST --url "${BASE_URL}/api/shorten" --unique-create
  --workers "$WORKERS" --timeout "$TIMEOUT" --out "$OUT/mixed_create.json")
redirect_cmd=(python3 "$DRIVER" --rate "$REDIRECT_RATE" --duration "$DURATION"
  --targets "$TARGETS" --workers "$WORKERS" --timeout "$TIMEOUT"
  --out "$OUT/mixed_redirect.json")

if [[ "$DRY_RUN" == "1" ]]; then
  create_cmd+=(--dry-run)
  redirect_cmd+=(--dry-run)
  echo "+ ${create_cmd[*]}"
  echo "+ ${redirect_cmd[*]}"
  "${create_cmd[@]}"
  "${redirect_cmd[@]}"
  exit 0
fi

echo "+ ${create_cmd[*]} &"
echo "+ ${redirect_cmd[*]} &"
"${create_cmd[@]}" &
pid1=$!
"${redirect_cmd[@]}" &
pid2=$!
ec=0
wait "$pid1" || ec=1
wait "$pid2" || ec=1
exit "$ec"
