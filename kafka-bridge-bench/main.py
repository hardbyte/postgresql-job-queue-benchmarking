"""Kafka→receiver bridge — the consumer-layer fan-out for the broker arm.

The Debezium connector reads the WAL once and writes to a single Kafka topic
per table. Fan-out happens *here*, at the consumer layer: one Kafka consumer
group per harness consumer, each reading the full topic independently and
POSTing the Debezium envelopes to the receiver's /sink/<cid>.

Insulation contrast with every other arm: a dead consumer does NOT pin the
source. Delivery uses blocking retry with no offset commit until the receiver
acks (200), so when a consumer is marked dead (receiver returns 503) its
group stops committing and its *Kafka offset lag* grows — while the connector
keeps consuming the WAL and advancing the single replication slot. The
backlog lives in the broker's log, not in the source's WAL.

Env: BOOTSTRAP, SINK_URL, CONSUMER_COUNT, TOPIC_PATTERN, POLL_MS, BATCH_MAX,
SAMPLE_EVERY_S. Stdout: descriptor + per-consumer offset_lag JSONL samples.
SIGTERM → exit 0.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

stop_event = threading.Event()


def _log(msg: str) -> None:
    print(f"[kafka-bridge] {msg}", file=sys.stderr, flush=True)


def _emit(record: dict) -> None:
    sys.stdout.write(json.dumps(record) + "\n")
    sys.stdout.flush()


def post_with_retry(url: str, events: list[dict]) -> bool:
    """Deliver one batch; retry with backoff until acked (200) or shutdown.
    Returns True if delivered, False if we stopped first. Not committing the
    Kafka offset until this returns True is what turns a dead consumer into
    growing offset lag rather than source-WAL retention."""
    if not events:
        return True
    body = json.dumps(events).encode()
    backoff_s = 0.1
    while not stop_event.is_set():
        req = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, urllib.error.HTTPError, OSError):
            pass
        stop_event.wait(backoff_s)
        backoff_s = min(backoff_s * 2, 1.0)
    return False


def consumer_loop(cid: int, bootstrap: str, sink_base: str, topic_pattern: str,
                  group_prefix: str, poll_ms: int, batch_max: int) -> None:
    try:
        _consumer_loop(cid, bootstrap, sink_base, topic_pattern, group_prefix,
                       poll_ms, batch_max)
    except Exception:
        _log(f"consumer {cid}: crashed")
        traceback.print_exc()
        os._exit(1)


def _consumer_loop(cid: int, bootstrap: str, sink_base: str, topic_pattern: str,
                   group_prefix: str, poll_ms: int, batch_max: int) -> None:
    sink_url = f"{sink_base}/sink/{cid}"
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id=f"{group_prefix}-{cid}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: b,
        consumer_timeout_ms=poll_ms,
        # Debezium creates the table topic only once the first row is
        # produced — after the bridge subscribes. Refresh metadata often so
        # pattern subscription discovers the new topic within seconds rather
        # than the 5-minute default.
        metadata_max_age_ms=5000,
    )
    consumer.subscribe(pattern=topic_pattern)
    _log(f"consumer {cid}: subscribed to /{topic_pattern}/")
    while not stop_event.is_set():
        batches = consumer.poll(timeout_ms=poll_ms, max_records=batch_max)
        if not batches:
            continue
        events: list[dict] = []
        for _tp, records in batches.items():
            for record in records:
                if record.value is None:  # tombstone
                    continue
                try:
                    events.append(json.loads(record.value))
                except json.JSONDecodeError:
                    _log(f"consumer {cid}: undecodable kafka value skipped")
        # Deliver, then commit — only advance the group's offset once the
        # receiver has the batch. On a dead consumer this blocks here and the
        # offset (hence lag) freezes.
        if post_with_retry(sink_url, events):
            consumer.commit()
        else:
            break  # stopped mid-retry; leave offset uncommitted
    consumer.close()
    _log(f"consumer {cid}: stopped")


def lag_sampler(bootstrap: str, consumer_count: int, group_prefix: str,
                every_s: float) -> None:
    """Emit per-consumer Kafka offset lag: end offset minus committed offset,
    summed over partitions. This is where a dead consumer's backlog shows up
    (the source slot stays flat)."""
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    probe = KafkaConsumer(bootstrap_servers=bootstrap, value_deserializer=lambda b: b)
    logged_error = False
    while not stop_event.wait(every_s):
        for cid in range(consumer_count):
            group = f"{group_prefix}-{cid}"
            try:
                # list_group_offsets returns {group: {TopicPartition: OffsetAndMetadata}}.
                committed = admin.list_group_offsets(group).get(group, {})
                tps = [tp for tp, om in committed.items() if om and om.offset >= 0]
                if not tps:
                    continue
                ends = probe.end_offsets(tps)
                lag = sum(max(0, ends.get(tp, committed[tp].offset) - committed[tp].offset)
                          for tp in tps)
            except Exception:
                if not logged_error:  # log once — don't spam every interval
                    _log("lag sampler error:")
                    traceback.print_exc()
                    logged_error = True
                continue
            _emit({"kind": "kafka", "subject_kind": "kafka_consumer",
                   "subject": f"consumer-{cid}", "instance_id": cid,
                   "metric": "offset_lag", "value": float(lag),
                   "window_s": 0.0})
    admin.close()
    probe.close()


def main() -> int:
    bootstrap = os.environ.get("BOOTSTRAP", "localhost:9092")
    sink_base = os.environ["SINK_URL"].rstrip("/")
    consumer_count = int(os.environ.get("CONSUMER_COUNT", "2"))
    topic_pattern = os.environ.get("TOPIC_PATTERN", r"cdcbench\.cdc_bench\..*")
    group_prefix = os.environ.get("GROUP_PREFIX", "cdc-bridge")
    poll_ms = int(os.environ.get("POLL_MS", "500"))
    batch_max = int(os.environ.get("BATCH_MAX", "500"))
    sample_every_s = float(os.environ.get("SAMPLE_EVERY_S", "5"))

    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())
    signal.signal(signal.SIGINT, lambda *_: stop_event.set())

    _emit({
        "kind": "descriptor",
        "system": "debezium-kafka",
        "topology": "broker",
        "bootstrap": bootstrap,
        "topic_pattern": topic_pattern,
        "consumer_groups": [f"{group_prefix}-{i}" for i in range(consumer_count)],
        "version": "in-repo kafka-python bridge",
        "schema_version": "1",
        "started_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    })

    threads = [
        threading.Thread(target=consumer_loop, name=f"consumer-{cid}",
                         args=(cid, bootstrap, sink_base, topic_pattern,
                               group_prefix, poll_ms, batch_max), daemon=True)
        for cid in range(consumer_count)
    ]
    threads.append(threading.Thread(
        target=lag_sampler, name="lag-sampler",
        args=(bootstrap, consumer_count, group_prefix, sample_every_s),
        daemon=True))
    for t in threads:
        t.start()
    while not stop_event.is_set():
        time.sleep(0.2)
    for t in threads:
        t.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
