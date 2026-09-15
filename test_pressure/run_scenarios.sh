#!/usr/bin/env bash
# Phase 0 entrypoint: seed (optional) and run fixed-rate scenarios.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/test_pressure/configs/common.env"

DRY_RUN=0
SCENARIO="all"
SKIP_SEED=0
RESULTS_DIR=""
LIST_ONLY=0

usage() {
  cat <<EOF
Usage: $0 [options]

Options:
  --dry-run              Print/run driver dry-run only; no real load if combined with missing server
  --scenario NAME        hot|uniform|zipf|herd|negative|mixed|deps|all (default: all)
  --rate N               Target arrival rate req/s (default: $RATE)
  --duration DUR         e.g. 15s (default: $DURATION)
  --base-url URL         Server base (default: $BASE_URL)
  --seed-count N         Codes to create when seeding (default: $SEED_COUNT)
  --skip-seed            Reuse existing results dir targets (requires --results)
  --results DIR          Results/targets directory
  --list                 List scenarios and exit
  -h, --help             Show help

Primary driver: test_pressure/drivers/fixed_rate_http.py (fixed arrival rate).
Secondary tools (webbench/wrk) are documented in docs/phase0_baseline.md; not invoked here.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1; shift ;;
    --scenario) SCENARIO="${2:-}"; shift 2 ;;
    --rate) RATE="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --base-url) BASE_URL="${2:-}"; shift 2 ;;
    --seed-count) SEED_COUNT="${2:-}"; shift 2 ;;
    --skip-seed) SKIP_SEED=1; shift ;;
    --results) RESULTS_DIR="${2:-}"; shift 2 ;;
    --list) LIST_ONLY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

if [[ "$LIST_ONLY" -eq 1 ]]; then
  cat <<EOF
hot       Single hot short code
uniform   Many codes, uniform
zipf      Zipf hotspot mix
herd      Cold cache / thundering herd
negative  Missing + business-expired
mixed     Mixed create + redirect
deps      Redis/Kafka down hooks
all       Run every scenario above
EOF
  exit 0
fi

if [[ -z "$RESULTS_DIR" ]]; then
  RESULTS_DIR="$ROOT/test_pressure/results/$(date +%Y%m%dT%H%M%S)"
fi
mkdir -p "$RESULTS_DIR/targets" "$RESULTS_DIR/runs"
export BASE_URL RATE DURATION WORKERS TIMEOUT DRY_RUN
export OUT="$RESULTS_DIR/runs"

server_up=0
if curl -fsS -m 2 "${BASE_URL}/health" >/dev/null 2>&1; then
  server_up=1
fi

echo "results: $RESULTS_DIR"
echo "base: $BASE_URL  rate=$RATE duration=$DURATION dry_run=$DRY_RUN server_up=$server_up"

if [[ "$SKIP_SEED" -eq 0 ]]; then
  if [[ "$DRY_RUN" -eq 1 ]]; then
    python3 "$ROOT/test_pressure/tools/seed_codes.py" --base "$BASE_URL" \
      --outdir "$RESULTS_DIR/targets" --count "$SEED_COUNT" --zipf-s "$ZIPF_S" --dry-run
    # Placeholder targets so scenario dry-runs have files
    echo "${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/hot_targets.txt"
    echo "${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/uniform_targets.txt"
    echo "1.0 ${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/zipf_targets.txt"
    echo "${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/herd_targets.txt"
    echo "${BASE_URL}/zzMISSING0" >"$RESULTS_DIR/targets/negative_targets.txt"
    echo '{"hot":"EXAMPLECODE"}' >"$RESULTS_DIR/targets/codes.json"
  elif [[ "$server_up" -eq 1 ]]; then
    python3 "$ROOT/test_pressure/tools/seed_codes.py" --base "$BASE_URL" \
      --outdir "$RESULTS_DIR/targets" --count "$SEED_COUNT" --zipf-s "$ZIPF_S"
  else
    echo "WARN: server not reachable at ${BASE_URL}/health; writing placeholder targets" >&2
    echo "${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/hot_targets.txt"
    cp "$RESULTS_DIR/targets/hot_targets.txt" "$RESULTS_DIR/targets/uniform_targets.txt"
    echo "1.0 ${BASE_URL}/EXAMPLECODE" >"$RESULTS_DIR/targets/zipf_targets.txt"
    cp "$RESULTS_DIR/targets/hot_targets.txt" "$RESULTS_DIR/targets/herd_targets.txt"
    echo "${BASE_URL}/zzMISSING0" >"$RESULTS_DIR/targets/negative_targets.txt"
    echo '{"hot":"EXAMPLECODE","note":"placeholders; seed when server is up"}' \
      >"$RESULTS_DIR/targets/codes.json"
    if [[ "$DRY_RUN" -eq 0 ]]; then
      echo "BLOCKER: cannot seed or load-test without server. Re-run with --dry-run or start deps." >&2
      echo "See docs/phase0_baseline.md. Scripts are present under test_pressure/." >&2
      # Still allow deps scenario notes + dry documentation path
      SCENARIO_FORCE_NOTES=1
    fi
  fi
fi

HOT_CODE=""
if [[ -f "$RESULTS_DIR/targets/codes.json" ]]; then
  HOT_CODE=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1])).get("hot",""))' \
    "$RESULTS_DIR/targets/codes.json" 2>/dev/null || true)
fi
export HOT_CODE

run_one() {
  local name="$1"
  case "$name" in
    hot)
      TARGETS="$RESULTS_DIR/targets/hot_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/01_hot.sh"
      ;;
    uniform)
      TARGETS="$RESULTS_DIR/targets/uniform_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/02_uniform.sh"
      ;;
    zipf)
      TARGETS="$RESULTS_DIR/targets/zipf_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/03_zipf.sh"
      ;;
    herd)
      TARGETS="$RESULTS_DIR/targets/herd_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/04_herd.sh"
      ;;
    negative)
      TARGETS="$RESULTS_DIR/targets/negative_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/05_negative.sh"
      ;;
    mixed)
      TARGETS="$RESULTS_DIR/targets/uniform_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/06_mixed.sh"
      ;;
    deps)
      TARGETS="$RESULTS_DIR/targets/hot_targets.txt" \
        bash "$ROOT/test_pressure/scenarios/07_deps_down.sh"
      ;;
    *)
      echo "unknown scenario: $name" >&2
      exit 2
      ;;
  esac
}

if [[ "${SCENARIO_FORCE_NOTES:-0}" -eq 1 && "$SCENARIO" == "all" ]]; then
  # Server down: still emit deps notes and dry-run driver checks
  DRY_RUN=1
  export DRY_RUN
  run_one deps
  for s in hot uniform zipf herd negative mixed; do
    run_one "$s" || true
  done
  echo "completed dry documentation pass with server down"
  exit 0
fi

if [[ "$SCENARIO" == "all" ]]; then
  for s in hot uniform zipf herd negative mixed deps; do
    echo "=== scenario $s ==="
    if [[ "$server_up" -eq 0 && "$DRY_RUN" -eq 0 ]]; then
      DRY_RUN=1 run_one "$s"
    else
      run_one "$s"
    fi
  done
else
  echo "=== scenario $SCENARIO ==="
  run_one "$SCENARIO"
fi

echo "done. results in $RESULTS_DIR"
