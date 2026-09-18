#!/usr/bin/env python3
"""
Consume short URL click events from Kafka and write them to MySQL.

Required packages:
  pip install confluent-kafka mysql-connector-python

The consumer commits offsets only after MySQL has accepted the event, or the
message is deemed permanently undeliverable (poison pill: undecodable, malformed
or rejected by a permanent MySQL constraint), in which case it is logged,
counted and skipped so a single bad message cannot stall the partition. The
`click_event.event_id` primary key makes repeated delivery idempotent.
"""

import json
import os
import signal
import sys
import threading
import time
from json import JSONDecodeError
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, Optional, Tuple

import mysql.connector
from confluent_kafka import Consumer, KafkaException


RUNNING = True

# MySQL errors that will fail identically on every retry (NULL/column
# constraints, unknown column/table, out-of-range or truncated data, check
# constraints): the message is a poison pill and must be skipped instead of
# crash-looping on it forever.
PERMANENT_MYSQL_ERRNOS = {1048, 1054, 1146, 1264, 1265, 1366, 1406, 3819}
MAX_TRANSIENT_RETRIES = 5
REQUIRED_EVENT_FIELDS = ("event_id", "short_code", "clicked_at_ms")


class ConsumerMetrics:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.events_consumed_total = 0
        self.events_inserted_total = 0
        self.events_invalid_total = 0
        self.events_failed_total = 0
        self.kafka_lag = 0
        self.last_commit_ts = 0.0

    def inc(self, name: str) -> None:
        with self.lock:
            setattr(self, name, getattr(self, name) + 1)

    def set_lag(self, value: int) -> None:
        with self.lock:
            self.kafka_lag = max(0, value)

    def mark_commit(self) -> None:
        with self.lock:
            self.last_commit_ts = time.time()

    def render(self) -> str:
        with self.lock:
            lines = [
                "# HELP shorturl_click_consumer_events_total Click consumer events by result.",
                "# TYPE shorturl_click_consumer_events_total counter",
                f'shorturl_click_consumer_events_total{{result="consumed"}} {self.events_consumed_total}',
                f'shorturl_click_consumer_events_total{{result="inserted"}} {self.events_inserted_total}',
                f'shorturl_click_consumer_events_total{{result="invalid"}} {self.events_invalid_total}',
                f'shorturl_click_consumer_events_total{{result="failed"}} {self.events_failed_total}',
                "# HELP shorturl_kafka_consumer_lag Kafka consumer lag across assigned partitions.",
                "# TYPE shorturl_kafka_consumer_lag gauge",
                f"shorturl_kafka_consumer_lag {self.kafka_lag}",
                "# HELP shorturl_click_consumer_last_commit_timestamp_seconds Last successful offset commit timestamp.",
                "# TYPE shorturl_click_consumer_last_commit_timestamp_seconds gauge",
                f"shorturl_click_consumer_last_commit_timestamp_seconds {self.last_commit_ts:.0f}",
                "",
            ]
        return "\n".join(lines)


METRICS = ConsumerMetrics()


def stop(_signum, _frame):
    global RUNNING
    RUNNING = False


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return
        body = METRICS.render().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format: str, *_args: Any) -> None:
        return


def start_metrics_server() -> None:
    port = int(os.getenv("METRICS_PORT", "9108"))
    server = HTTPServer((os.getenv("METRICS_HOST", "127.0.0.1"), port), MetricsHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()


def log_json(event: str, **fields: Any) -> None:
    record = {"event": event, **fields}
    print(json.dumps(record, ensure_ascii=False, separators=(",", ":")), flush=True)


def parse_event(msg: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Decode and validate one Kafka message; never raises.

    Returns (event, None) when the message is a valid click event, or
    (None, reason) when it can never be one (bad encoding, malformed JSON,
    missing required fields, unparsable clicked_at_ms) and should be skipped.
    """
    try:
        raw = msg.value().decode("utf-8")
    except UnicodeDecodeError as exc:
        return None, f"invalid utf-8: {exc}"
    try:
        event = json.loads(raw)
    except JSONDecodeError as exc:
        return None, f"invalid json: {exc}"
    if not isinstance(event, dict):
        return None, f"expected a JSON object, got {type(event).__name__}"
    for field in REQUIRED_EVENT_FIELDS:
        if field not in event:
            return None, f"missing required field: {field}"
    try:
        int(event["clicked_at_ms"])
    except (TypeError, ValueError) as exc:
        return None, f"clicked_at_ms is not an integer: {exc}"
    return event, None


def mysql_conn():
    return mysql.connector.connect(
        host=os.getenv("MYSQL_HOST", "127.0.0.1"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "shorturl"),
        password=os.getenv("MYSQL_PASSWORD", "shorturl"),
        database=os.getenv("MYSQL_DATABASE", "shorturl"),
    )


def insert_event(conn, event: Dict[str, Any]) -> None:
    sql = """
        INSERT INTO click_event
            (event_id, short_code, clicked_at_ms, user_agent, referer, x_forwarded_for)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE event_id = event_id
    """
    values = (
        event["event_id"],
        event["short_code"],
        int(event["clicked_at_ms"]),
        event.get("user_agent") or None,
        event.get("referer") or None,
        event.get("x_forwarded_for") or None,
    )
    cursor = conn.cursor()
    cursor.execute(sql, values)
    conn.commit()
    cursor.close()


def insert_once(conn, event: Dict[str, Any]) -> bool:
    """Insert one event. True means the message may be committed.

    Permanent MySQL errors (and any residual structural badness) are treated
    as poison pills: logged, counted as invalid, and skipped. Transient MySQL
    errors propagate to the caller, which retries with a fresh connection.
    """
    try:
        insert_event(conn, event)
    except mysql.connector.Error as exc:
        if exc.errno in PERMANENT_MYSQL_ERRNOS:
            METRICS.inc("events_invalid_total")
            log_json(
                "click_event_poison",
                error=str(exc),
                errno=exc.errno,
                event_id=event.get("event_id"),
                short_code=event.get("short_code"),
            )
            return True
        raise
    except (KeyError, TypeError, ValueError) as exc:
        METRICS.inc("events_invalid_total")
        log_json(
            "click_event_poison",
            error=str(exc),
            event_id=event.get("event_id") if isinstance(event, dict) else None,
        )
        return True
    METRICS.inc("events_inserted_total")
    return True


def process_message(state: Dict[str, Any], msg: Any) -> bool:
    """Handle one Kafka message. True = safe to commit, False = give up.

    Invalid messages are counted and skipped (still committable). Transient
    MySQL errors are retried up to MAX_TRANSIENT_RETRIES with exponential
    backoff, refreshing the connection in state["conn"] between attempts.
    """
    event, reason = parse_event(msg)
    if event is None:
        METRICS.inc("events_invalid_total")
        log_json(
            "click_event_invalid",
            error=reason,
            partition=msg.partition(),
            offset=msg.offset(),
        )
        return True

    for attempt in range(MAX_TRANSIENT_RETRIES + 1):
        try:
            if not insert_once(state["conn"], event):
                return False
            log_json(
                "click_event_committed",
                event_id=event.get("event_id"),
                short_code=event.get("short_code"),
                partition=msg.partition(),
                offset=msg.offset(),
            )
            return True
        except mysql.connector.Error as exc:
            METRICS.inc("events_failed_total")
            log_json(
                "click_event_retry",
                error=str(exc),
                attempt=attempt + 1,
                max_attempts=MAX_TRANSIENT_RETRIES + 1,
            )
            time.sleep(min(0.1 * 2 ** attempt, 2.0))
            try:
                state["conn"] = mysql_conn()
            except Exception as reconnect_exc:
                log_json("click_event_reconnect_failed", error=str(reconnect_exc))
    return False


def update_lag(consumer: Consumer) -> None:
    total = 0
    try:
        positions = consumer.position(consumer.assignment())
        for tp in positions:
            if tp.offset < 0:
                continue
            _low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
            total += max(0, high - tp.offset)
        METRICS.set_lag(total)
    except KafkaException:
        return


def main() -> int:
    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    start_metrics_server()

    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BROKERS", "127.0.0.1:9092"),
        "group.id": os.getenv("KAFKA_GROUP_ID", "shorturl-click-writer"),
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    topic = os.getenv("KAFKA_CLICK_TOPIC", "shorturl.clicks")
    consumer.subscribe([topic])

    conn_state = {"conn": mysql_conn()}
    try:
        while RUNNING:
            msg = consumer.poll(1.0)
            if msg is None:
                update_lag(consumer)
                continue
            err = msg.error()
            if err is not None:
                if err.fatal():
                    log_json("click_event_fatal", error=str(err))
                    return 1
                # Transient consumer errors (e.g. _ALL_BROKERS_DOWN during a
                # rolling broker restart): log and keep polling.
                log_json("click_event_error_ignored", error=str(err))
                continue

            METRICS.inc("events_consumed_total")
            if not process_message(conn_state, msg):
                # Retries exhausted without success: exit without committing
                # so the offset is retried after restart. Poison pills never
                # get here (they return True and are committed as skipped).
                log_json(
                    "click_event_giving_up",
                    partition=msg.partition(),
                    offset=msg.offset(),
                )
                return 1
            consumer.commit(message=msg, asynchronous=False)
            METRICS.mark_commit()
            update_lag(consumer)
    finally:
        conn_state["conn"].close()
        consumer.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
