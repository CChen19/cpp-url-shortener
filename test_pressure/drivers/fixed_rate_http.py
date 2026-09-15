#!/usr/bin/env python3
"""Fixed arrival-rate HTTP driver (primary Phase 0 load tool).

Open-loop: schedules requests at a target rate and records per-status latency.
Does not claim QPS capacity; report correct-status throughput from the summary.
"""
from __future__ import annotations

import argparse
import json
import math
import random
import sys
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple


@dataclass
class Sample:
    status: int
    latency_ms: float
    error: str = ""


@dataclass
class Summary:
    requested: int = 0
    completed: int = 0
    timed_out: int = 0
    transport_errors: int = 0
    by_status: Counter = field(default_factory=Counter)
    latencies_ms: List[float] = field(default_factory=list)

    def to_dict(self, rate: float, duration_s: float) -> dict:
        lat = sorted(self.latencies_ms)
        def pct(p: float) -> Optional[float]:
            if not lat:
                return None
            idx = min(len(lat) - 1, max(0, int(math.ceil(p / 100.0 * len(lat)) - 1)))
            return round(lat[idx], 3)

        completed = max(1, self.completed)
        return {
            "driver": "fixed_rate_http.py",
            "target_rate_rps": rate,
            "duration_s": duration_s,
            "requested": self.requested,
            "completed": self.completed,
            "timed_out": self.timed_out,
            "transport_errors": self.transport_errors,
            "timeout_rate": round(self.timed_out / completed, 6),
            "error_rate": round(self.transport_errors / completed, 6),
            "status_counts": {str(k): v for k, v in sorted(self.by_status.items())},
            "correct_status_throughput_rps": {
                str(k): round(v / duration_s, 3) for k, v in sorted(self.by_status.items())
            },
            "latency_ms": {
                "p50": pct(50),
                "p95": pct(95),
                "p99": pct(99),
                "min": round(lat[0], 3) if lat else None,
                "max": round(lat[-1], 3) if lat else None,
                "count": len(lat),
            },
        }


def parse_duration(text: str) -> float:
    text = text.strip().lower()
    if text.endswith("ms"):
        return float(text[:-2]) / 1000.0
    if text.endswith("s"):
        return float(text[:-1])
    if text.endswith("m"):
        return float(text[:-1]) * 60.0
    return float(text)


def percentile_ready_body(body: Optional[str], body_file: Optional[str]) -> Optional[bytes]:
    if body_file:
        with open(body_file, "rb") as fh:
            return fh.read()
    if body is not None:
        return body.encode("utf-8")
    return None


def load_targets(path: Optional[str], url: Optional[str]) -> List[str]:
    if url:
        return [url]
    if not path:
        raise SystemExit("provide --url or --targets")
    urls: List[str] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # weight url  OR  url
            parts = line.split()
            if len(parts) == 1:
                urls.append(parts[0])
            elif len(parts) >= 2 and parts[0].replace(".", "", 1).isdigit():
                weight = float(parts[0])
                target = parts[1]
                # expand lightly; zipf files should already be expanded
                n = max(1, int(round(weight)))
                urls.extend([target] * n)
            else:
                urls.append(parts[-1])
    if not urls:
        raise SystemExit(f"no targets in {path}")
    return urls


def load_weighted_targets(path: str) -> List[Tuple[float, str]]:
    items: List[Tuple[float, str]] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 2 and parts[0].replace(".", "", 1).isdigit():
                items.append((float(parts[0]), parts[1]))
            else:
                items.append((1.0, parts[0]))
    if not items:
        raise SystemExit(f"no weighted targets in {path}")
    return items


def one_request(
    method: str,
    url: str,
    headers: Dict[str, str],
    body: Optional[bytes],
    timeout_s: float,
) -> Sample:
    req = urllib.request.Request(url, data=body if method != "GET" else None, method=method)
    for k, v in headers.items():
        req.add_header(k, v)
    t0 = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            resp.read()
            status = getattr(resp, "status", resp.getcode())
            return Sample(status=int(status), latency_ms=(time.perf_counter() - t0) * 1000.0)
    except urllib.error.HTTPError as e:
        try:
            e.read()
        except Exception:
            pass
        return Sample(status=int(e.code), latency_ms=(time.perf_counter() - t0) * 1000.0)
    except urllib.error.URLError as e:
        msg = str(getattr(e, "reason", e))
        if "timed out" in msg.lower() or "timeout" in msg.lower():
            return Sample(status=0, latency_ms=(time.perf_counter() - t0) * 1000.0, error="timeout")
        return Sample(status=0, latency_ms=(time.perf_counter() - t0) * 1000.0, error=msg)
    except TimeoutError:
        return Sample(status=0, latency_ms=(time.perf_counter() - t0) * 1000.0, error="timeout")
    except Exception as e:  # noqa: BLE001 — driver must keep running
        return Sample(status=0, latency_ms=(time.perf_counter() - t0) * 1000.0, error=str(e))


def run_fixed_rate(
    *,
    rate: float,
    duration_s: float,
    method: str,
    urls: Sequence[str],
    weighted: Optional[Sequence[Tuple[float, str]]],
    headers: Dict[str, str],
    body: Optional[bytes],
    timeout_s: float,
    workers: int,
    body_factory=None,
) -> Summary:
    summary = Summary()
    lock = threading.Lock()
    stop_at = time.perf_counter() + duration_s
    interval = 1.0 / rate if rate > 0 else duration_s
    rng = random.Random(42)

    weights: Optional[List[float]] = None
    targets: List[str]
    if weighted:
        weights = [w for w, _ in weighted]
        targets = [u for _, u in weighted]
    else:
        targets = list(urls)

    def pick_url() -> str:
        if weights is None:
            return targets[rng.randrange(len(targets))]
        return rng.choices(targets, weights=weights, k=1)[0]

    def worker_job(url: str, payload: Optional[bytes]) -> None:
        sample = one_request(method, url, headers, payload, timeout_s)
        with lock:
            summary.completed += 1
            if sample.error == "timeout":
                summary.timed_out += 1
            elif sample.error:
                summary.transport_errors += 1
            if sample.status:
                summary.by_status[sample.status] += 1
            else:
                summary.by_status[0] += 1
            summary.latencies_ms.append(sample.latency_ms)

    next_t = time.perf_counter()
    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        while True:
            now = time.perf_counter()
            if now >= stop_at:
                break
            if now < next_t:
                time.sleep(min(0.001, next_t - now))
                continue
            url = pick_url()
            payload = body_factory() if body_factory else body
            summary.requested += 1
            pool.submit(worker_job, url, payload)
            next_t += interval
            # If we fall behind, skip forward to avoid catch-up storm
            if next_t < time.perf_counter() - interval:
                next_t = time.perf_counter()
        pool.shutdown(wait=True)
    return summary


def build_headers(pairs: Sequence[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for item in pairs:
        if ":" not in item:
            raise SystemExit(f"bad --header {item!r}; expected 'Name: value'")
        k, v = item.split(":", 1)
        out[k.strip()] = v.strip()
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rate", type=float, required=True, help="target arrival rate (req/s)")
    p.add_argument("--duration", required=True, help="e.g. 10s, 500ms, 2m")
    p.add_argument("--method", default="GET", choices=["GET", "POST", "PUT", "DELETE"])
    p.add_argument("--url", help="single URL")
    p.add_argument("--targets", help="file of URLs (optional 'weight url' lines)")
    p.add_argument("--weighted-targets", help="file of 'weight url' lines sampled each request")
    p.add_argument("--body", help="request body string")
    p.add_argument("--body-file", help="request body from file")
    p.add_argument("--unique-create", action="store_true",
                   help="POST JSON long_url with unique suffix each request")
    p.add_argument("--header", action="append", default=[], help="Name: value")
    p.add_argument("--timeout", default="2s", help="per-request timeout")
    p.add_argument("--workers", type=int, default=64)
    p.add_argument("--out", help="write JSON summary to path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    duration_s = parse_duration(args.duration)
    timeout_s = parse_duration(args.timeout)
    headers = build_headers(args.header)
    body = percentile_ready_body(args.body, args.body_file)
    weighted = load_weighted_targets(args.weighted_targets) if args.weighted_targets else None
    urls = [] if weighted else load_targets(args.targets, args.url)

    body_factory = None
    if args.unique_create:
        headers.setdefault("Content-Type", "application/json")
        counter = {"n": 0}
        lock = threading.Lock()

        def body_factory() -> bytes:
            with lock:
                counter["n"] += 1
                n = counter["n"]
            payload = {"long_url": f"https://example.com/bench-{n}-{time.time_ns()}"}
            return json.dumps(payload).encode("utf-8")

    if args.dry_run:
        print(json.dumps({
            "dry_run": True,
            "rate": args.rate,
            "duration_s": duration_s,
            "method": args.method,
            "url_count": len(weighted) if weighted else len(urls),
            "workers": args.workers,
        }, indent=2))
        return 0

    summary = run_fixed_rate(
        rate=args.rate,
        duration_s=duration_s,
        method=args.method,
        urls=urls,
        weighted=weighted,
        headers=headers,
        body=body,
        timeout_s=timeout_s,
        workers=args.workers,
        body_factory=body_factory,
    )
    payload = summary.to_dict(args.rate, duration_s)
    text = json.dumps(payload, indent=2)
    print(text)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as fh:
            fh.write(text)
            fh.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
