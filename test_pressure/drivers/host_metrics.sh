#!/usr/bin/env bash
# Sample CPU%, RSS, and context switches for a PID.
set -euo pipefail

PID=""
INTERVAL=1
OUT=""
DURATION=""

usage() {
  echo "usage: $0 --pid PID [--interval SEC] [--duration SEC] [--out FILE]" >&2
  exit 2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pid) PID="${2:-}"; shift 2 ;;
    --interval) INTERVAL="${2:-}"; shift 2 ;;
    --duration) DURATION="${2:-}"; shift 2 ;;
    --out) OUT="${2:-}"; shift 2 ;;
    -h|--help) usage ;;
    *) usage ;;
  esac
done

[[ -n "$PID" ]] || usage
[[ -r "/proc/$PID/stat" ]] || { echo "pid $PID not readable" >&2; exit 1; }

PAGESIZE=$(getconf PAGESIZE)
header="ts_epoch,pid,cpu_pct,rss_kb,voluntary_ctxt,nonvoluntary_ctxt"
if [[ -n "$OUT" ]]; then
  echo "$header" >"$OUT"
else
  echo "$header"
fi

prev_total=0
prev_proc=0
first=1
start=$(date +%s)

while true; do
  if [[ ! -r "/proc/$PID/stat" ]]; then
    echo "pid $PID gone" >&2
    break
  fi

  read -r utime stime rss_pages < <(awk '{print $14, $15, $24}' "/proc/$PID/stat")
  rss_kb=$((rss_pages * PAGESIZE / 1024))
  vol=0
  nonvol=0
  if [[ -r "/proc/$PID/status" ]]; then
    vol=$(awk '/^voluntary_ctxt_switches:/ {print $2}' "/proc/$PID/status")
    nonvol=$(awk '/^nonvoluntary_ctxt_switches:/ {print $2}' "/proc/$PID/status")
  fi

  read -r total < <(awk '/^cpu / {print $2+$3+$4+$5+$6+$7+$8+$9}' /proc/stat)
  proc=$((utime + stime))
  cpu_pct="0.00"
  if [[ $first -eq 0 ]]; then
    dtotal=$((total - prev_total))
    dproc=$((proc - prev_proc))
    if [[ $dtotal -gt 0 ]]; then
      cpu_pct=$(awk -v dproc="$dproc" -v dtotal="$dtotal" 'BEGIN { printf "%.2f", (100.0 * dproc / dtotal) }')
    fi
  fi
  first=0
  prev_total=$total
  prev_proc=$proc

  ts=$(date +%s)
  line="$ts,$PID,$cpu_pct,$rss_kb,$vol,$nonvol"
  if [[ -n "$OUT" ]]; then
    echo "$line" >>"$OUT"
  else
    echo "$line"
  fi

  if [[ -n "$DURATION" ]]; then
    now=$(date +%s)
    if (( now - start >= DURATION )); then
      break
    fi
  fi
  sleep "$INTERVAL"
done
