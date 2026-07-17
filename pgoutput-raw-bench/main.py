"""pgoutput-raw baseline CDC adapter (docs/cdc-harness-design.md §2).

The "no insulation" arm: one logical replication slot per consumer, each
consumer thread reads its slot, normalizes to the canonical envelope, and
POSTs to the harness receiver. The slot is only advanced after the receiver
acknowledges delivery, so a dead consumer pins restart_lsn — exactly the
production hazard the insulation matrix measures — and delivery is
at-least-once across crashes.

M1 note: reads via `pg_logical_slot_peek_binary_changes` + explicit
`pg_replication_slot_advance` (SQL polling) rather than a streaming
START_REPLICATION session — psycopg3 has no replication-connection support.
Polling cadence is POLL_MS (default 50 ms), which bounds the floor of the
measured e2e latency; recorded in the descriptor so runs are attributable.

Env in: DATABASE_URL, SINK_URL, CONSUMER_COUNT, POLL_MS, PEEK_LIMIT.
Stdout: single descriptor line. Stderr: logs. SIGTERM → exit 0.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from cdc_harness.pgoutput import UNCHANGED_TOAST, Relation, parse_message  # noqa: E402

PUBLICATION = "cdc_pub"
SLOT_PREFIX = "cdc_raw_"
SOURCE_TABLE = "cdc_bench.events"

stop_event = threading.Event()


def _log(msg: str) -> None:
    print(f"[pgoutput-raw] {msg}", file=sys.stderr, flush=True)


def canonical_event(change) -> dict | None:
    if change.relation.qualified != SOURCE_TABLE:
        return None
    values = change.values

    def _int(name: str) -> int | None:
        raw = values.get(name)
        if raw is None or raw == UNCHANGED_TOAST:
            return None
        return int(raw)

    return {
        "table": change.relation.qualified,
        "op": change.op,
        "pk": _int("pk"),
        "seq": _int("seq"),
        "tx_id": _int("tx_id"),
        "emitted_us": _int("emitted_us"),
    }


def post_with_retry(url: str, events: list[dict]) -> None:
    """Deliver one batch; retry with backoff until acked or shutdown.

    Retrying without advancing the slot is what turns receiver 503s
    (consumer-dead chaos) into slot retention on the source.
    """
    body = json.dumps(events).encode()
    backoff_s = 0.1
    while not stop_event.is_set():
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    return
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            pass
        time.sleep(backoff_s)
        backoff_s = min(backoff_s * 2, 1.0)


def consumer_loop(consumer_id: int, database_url: str, sink_base: str,
                  poll_ms: int, peek_limit: int) -> None:
    slot = f"{SLOT_PREFIX}{consumer_id}"
    sink_url = f"{sink_base}/sink/{consumer_id}"
    conn = psycopg.connect(database_url, autocommit=True)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot,)
        )
        if cur.fetchone() is None:
            cur.execute(
                "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')", (slot,)
            )
            _log(f"consumer {consumer_id}: created slot {slot}")
    relations: dict[int, Relation] = {}
    while not stop_event.is_set():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT lsn::text, data FROM pg_logical_slot_peek_binary_changes("
                " %s, NULL, %s,"
                " VARIADIC ARRAY['proto_version','1','publication_names',%s])",
                (slot, peek_limit, PUBLICATION),
            )
            rows = cur.fetchall()
        if not rows:
            time.sleep(poll_ms / 1000.0)
            continue
        events: list[dict] = []
        for _lsn, data in rows:
            change = parse_message(bytes(data), relations)
            if change is not None:
                event = canonical_event(change)
                if event is not None:
                    events.append(event)
        if events:
            post_with_retry(sink_url, events)
        if stop_event.is_set():
            break
        last_lsn = rows[-1][0]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_replication_slot_advance(%s, %s::pg_lsn)", (slot, last_lsn)
            )
    conn.close()
    _log(f"consumer {consumer_id}: stopped")


def main() -> int:
    database_url = os.environ["DATABASE_URL"]
    sink_base = os.environ["SINK_URL"].rstrip("/")
    consumer_count = int(os.environ.get("CONSUMER_COUNT", "2"))
    poll_ms = int(os.environ.get("POLL_MS", "50"))
    peek_limit = int(os.environ.get("PEEK_LIMIT", "500"))

    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    descriptor = {
        "kind": "descriptor",
        "system": "pgoutput-raw",
        "db_name": database_url.rsplit("/", 1)[-1],
        "slot_names": [f"{SLOT_PREFIX}{i}" for i in range(consumer_count)],
        "publication": PUBLICATION,
        "event_tables": [SOURCE_TABLE],
        "extensions": [],
        "version": f"in-repo sql-poll (poll_ms={poll_ms}, peek_limit={peek_limit})",
        "schema_version": "1",
        "started_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    }
    print(json.dumps(descriptor), flush=True)

    threads = [
        threading.Thread(
            target=consumer_loop,
            args=(i, database_url, sink_base, poll_ms, peek_limit),
            name=f"consumer-{i}",
            daemon=True,
        )
        for i in range(consumer_count)
    ]
    for t in threads:
        t.start()
    while not stop_event.is_set():
        time.sleep(0.2)
    for t in threads:
        t.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
