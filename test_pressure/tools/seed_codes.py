#!/usr/bin/env python3
"""Seed short codes and write target lists for Phase 0 scenarios."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import List, Optional


def post_shorten(base: str, long_url: str, expire_at: Optional[str] = None) -> dict:
    payload = {"long_url": long_url}
    if expire_at is not None:
        payload["expire_at"] = expire_at
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        base.rstrip("/") + "/api/shorten",
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def write_lines(path: str, lines: List[str]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")


def zipf_weights(n: int, s: float) -> List[float]:
    raw = [1.0 / (i ** s) for i in range(1, n + 1)]
    total = sum(raw)
    return [w / total for w in raw]


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--base", default="http://127.0.0.1:9006")
    p.add_argument("--outdir", required=True)
    p.add_argument("--count", type=int, default=100)
    p.add_argument("--zipf-s", type=float, default=1.2)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    out = args.outdir
    os.makedirs(out, exist_ok=True)

    if args.dry_run:
        print(json.dumps({"dry_run": True, "outdir": out, "count": args.count}, indent=2))
        return 0

    codes: List[str] = []
    for i in range(args.count):
        body = post_shorten(args.base, f"https://example.com/phase0-seed-{i}-{time.time_ns()}")
        codes.append(body["short_code"])

    hot = codes[0]
    write_lines(os.path.join(out, "hot_targets.txt"), [f"{args.base}/{hot}"])
    write_lines(
        os.path.join(out, "uniform_targets.txt"),
        [f"{args.base}/{c}" for c in codes],
    )

    weights = zipf_weights(len(codes), args.zipf_s)
    zipf_lines = [f"{w:.8f} {args.base}/{c}" for w, c in zip(weights, codes)]
    write_lines(os.path.join(out, "zipf_targets.txt"), zipf_lines)

    # Business-expired: expire_at in the past
    expired = post_shorten(
        args.base,
        f"https://example.com/phase0-expired-{time.time_ns()}",
        expire_at="2000-01-01 00:00:00",
    )["short_code"]
    missing = "zzMISSING0"
    write_lines(
        os.path.join(out, "negative_targets.txt"),
        [
            f"{args.base}/{missing}",
            f"{args.base}/{expired}",
        ],
    )
    write_lines(
        os.path.join(out, "herd_targets.txt"),
        [f"{args.base}/{hot}"],
    )
    with open(os.path.join(out, "codes.json"), "w", encoding="utf-8") as fh:
        json.dump({
            "hot": hot,
            "codes": codes,
            "expired": expired,
            "missing": missing,
            "base": args.base,
        }, fh, indent=2)
        fh.write("\n")

    print(json.dumps({"seeded": len(codes), "hot": hot, "expired": expired, "outdir": out}, indent=2))
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except urllib.error.URLError as e:
        print(f"seed failed: {e}", file=sys.stderr)
        sys.exit(1)
