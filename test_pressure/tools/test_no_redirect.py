#!/usr/bin/env python3
"""Assert fixed_rate_http records first-hop 302 (does not follow Location).

Starts a tiny stdlib HTTP server that returns 302 -> /dest (200), then checks
that one_request / a short fixed-rate run counts status 302, not 200.
"""
from __future__ import annotations

import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "test_pressure", "drivers"))

import fixed_rate_http as driver  # noqa: E402


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # noqa: A003
        return

    def do_GET(self):  # noqa: N802
        if self.path.startswith("/redir"):
            self.send_response(302)
            self.send_header("Location", "/dest")
            self.end_headers()
            return
        if self.path.startswith("/dest"):
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
            return
        self.send_response(404)
        self.end_headers()


def main() -> int:
    server = HTTPServer(("127.0.0.1", 0), Handler)
    host, port = server.server_address
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base = f"http://{host}:{port}"

    # Contrast: default urllib follows to 200
    followed = driver.urllib.request.urlopen(base + "/redir", timeout=2)
    followed_status = getattr(followed, "status", followed.getcode())
    followed.close()
    if int(followed_status) != 200:
        print(f"UNEXPECTED: default urllib status={followed_status}, want 200", file=sys.stderr)
        server.shutdown()
        return 1

    sample = driver.one_request("GET", base + "/redir", {}, None, 2.0)
    if sample.status != 302:
        print(f"FAIL one_request: status={sample.status} error={sample.error!r}; want 302",
              file=sys.stderr)
        server.shutdown()
        return 1

    summary = driver.run_fixed_rate(
        rate=20,
        duration_s=0.5,
        method="GET",
        urls=[base + "/redir"],
        weighted=None,
        headers={},
        body=None,
        timeout_s=2.0,
        workers=8,
    )
    payload = summary.to_dict(20, 0.5)
    counts = payload["status_counts"]
    if counts.get("302", 0) < 1 or counts.get("200", 0) != 0:
        print(f"FAIL fixed-rate status_counts={counts}; want 302>0 and 200==0", file=sys.stderr)
        print(json.dumps(payload, indent=2), file=sys.stderr)
        server.shutdown()
        return 1

    server.shutdown()
    print(json.dumps({
        "ok": True,
        "default_urllib_followed_status": followed_status,
        "one_request_status": sample.status,
        "status_counts": counts,
        "completed": summary.completed,
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
