"""Run the same Kafka client workload against kafgres and Apache Kafka."""

from __future__ import annotations

import argparse
import json
import math
import platform
import struct
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import confluent_kafka
import psycopg
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

ROOT = Path(__file__).resolve().parent.parent
KAFGRES_SOURCE = ROOT / "broker-bench" / "vendor" / "kafgres"
KAFKA_IMAGE = "apache/kafka:4.3.1"
BACKENDS = {
    "kafgres-segment": (
        "127.0.0.1:19092",
        "postgres://postgres:postgres@127.0.0.1:15416/postgres",
    ),
    "kafgres-table": (
        "127.0.0.1:19192",
        "postgres://postgres:postgres@127.0.0.1:15417/postgres",
    ),
    "kafka": ("127.0.0.1:19292", None),
}
HEADER = struct.Struct("!QQ")


class LatencyHistogram:
    """Bounded, logarithmic buckets with about 1% relative resolution."""

    def __init__(self) -> None:
        self.counts: dict[int, int] = {}
        self.total = 0

    def record_ns(self, duration_ns: int) -> None:
        if duration_ns < 0:
            raise ValueError("latency cannot be negative")
        bucket = int(math.log1p(duration_ns) / math.log1p(0.01))
        self.counts[bucket] = self.counts.get(bucket, 0) + 1
        self.total += 1

    def percentile_ms(self, percentile: float) -> float | None:
        if not self.total:
            return None
        target = max(1, math.ceil(self.total * percentile / 100))
        observed = 0
        for bucket, count in sorted(self.counts.items()):
            observed += count
            if observed >= target:
                return math.expm1(bucket * math.log1p(0.01)) / 1_000_000
        raise AssertionError("histogram count mismatch")

    def summary(self) -> dict[str, float | None]:
        return {
            "p50_ms": self.percentile_ms(50),
            "p95_ms": self.percentile_ms(95),
            "p99_ms": self.percentile_ms(99),
        }


@dataclass(frozen=True)
class Workload:
    messages: int
    payload_bytes: int
    partitions: int
    timeout_s: float
    work_ms: float
    rate_s: float


def _version(backend: str, pg_url: str | None) -> dict[str, str]:
    if pg_url is None:
        return {"image": KAFKA_IMAGE}
    revision = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=KAFGRES_SOURCE,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with psycopg.connect(pg_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT current_setting('server_version'), extversion, "
                "current_setting('kafgres.storage_engine'), "
                "current_setting('kafgres.fsync_before_ack'), "
                "current_setting('kafgres.relaxed_produce_commit') "
                "FROM pg_extension WHERE extname = 'kafgres'"
            )
            row = cursor.fetchone()
    if row is None:
        raise RuntimeError(f"{backend}: kafgres extension is not installed")
    return dict(
        zip(
            (
                "postgres",
                "extension",
                "storage_engine",
                "fsync_before_ack",
                "relaxed_produce_commit",
                "source_sha",
            ),
            (*row, revision),
        )
    )


def _create_topic(
    admin: AdminClient, topic: str, partitions: int, timeout_s: float
) -> None:
    admin.create_topics(
        [NewTopic(topic, num_partitions=partitions, replication_factor=1)],
        operation_timeout=timeout_s,
    )[topic].result(timeout=timeout_s)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        metadata = admin.list_topics(topic=topic, timeout=min(5, timeout_s))
        topic_metadata = metadata.topics.get(topic)
        if (
            topic_metadata is not None
            and topic_metadata.error is None
            and len(topic_metadata.partitions) == partitions
        ):
            return
        time.sleep(0.1)
    raise TimeoutError(f"topic {topic} did not become ready")


def run_case(backend: str, workload: Workload) -> dict:
    bootstrap, pg_url = BACKENDS[backend]
    version = _version(backend, pg_url)
    topic = f"bench_{backend.replace('-', '_')}_{uuid.uuid4().hex[:12]}"
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})
    _create_topic(admin, topic, workload.partitions, workload.timeout_s)

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": topic,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
            "socket.timeout.ms": 10000,
        }
    )
    assigned = threading.Event()
    consumer.subscribe([topic], on_assign=lambda _consumer, _partitions: assigned.set())
    seen = bytearray(workload.messages)
    delivery_latency = LatencyHistogram()
    ack_latency = LatencyHistogram()
    errors: list[str] = []
    counters = {"acked": 0, "consumed": 0, "duplicates": 0, "invalid": 0}
    sent_count = 0
    deadline = time.monotonic() + workload.timeout_s

    def consume() -> None:
        pending_commit = 0
        try:
            while (
                counters["consumed"] < workload.messages and time.monotonic() < deadline
            ):
                message = consumer.poll(0.2)
                if message is None:
                    continue
                if message.error():
                    errors.append(f"consume: {message.error()}")
                    break
                value = message.value() or b""
                if len(value) != workload.payload_bytes:
                    counters["invalid"] += 1
                    continue
                sequence, sent_ns = HEADER.unpack_from(value)
                if sequence >= workload.messages:
                    counters["invalid"] += 1
                    continue
                if seen[sequence]:
                    counters["duplicates"] += 1
                else:
                    seen[sequence] = 1
                    counters["consumed"] += 1
                    delivery_latency.record_ns(time.monotonic_ns() - sent_ns)
                if workload.work_ms:
                    time.sleep(workload.work_ms / 1000)
                pending_commit += 1
                if pending_commit >= 100:
                    consumer.commit(asynchronous=False)
                    pending_commit = 0
            if pending_commit:
                consumer.commit(asynchronous=False)
        except Exception as exc:
            errors.append(f"consumer: {exc}")
        finally:
            consumer.close()

    consumer_thread = threading.Thread(target=consume, name=f"consumer-{backend}")
    consumer_thread.start()
    if not assigned.wait(timeout=min(15, workload.timeout_s)):
        errors.append("consumer group did not receive an assignment")

    producer_started = time.monotonic()
    producer_finished = producer_started
    if not errors:
        producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 5,
                "batch.num.messages": 1000,
                "message.timeout.ms": int(workload.timeout_s * 1000),
            }
        )

        def acknowledged(error, message) -> None:
            if error is not None:
                errors.append(f"produce: {error}")
            else:
                counters["acked"] += 1
                _, sent_ns = HEADER.unpack_from(message.value())
                ack_latency.record_ns(time.monotonic_ns() - sent_ns)

        padding = b"x" * (workload.payload_bytes - HEADER.size)
        for sequence in range(workload.messages):
            if errors or time.monotonic() >= deadline:
                break
            if workload.rate_s:
                wait_s = (
                    producer_started + sequence / workload.rate_s - time.monotonic()
                )
                if wait_s > 0:
                    time.sleep(wait_s)
            sent_ns = time.monotonic_ns()
            value = HEADER.pack(sequence, sent_ns) + padding
            while True:
                try:
                    producer.produce(
                        topic,
                        key=sequence.to_bytes(8, "big"),
                        value=value,
                        on_delivery=acknowledged,
                    )
                    sent_count += 1
                    break
                except BufferError:
                    producer.poll(0.05)
            producer.poll(0)
        remaining = producer.flush(max(0, deadline - time.monotonic()))
        if remaining:
            errors.append(f"{remaining} messages remained in the producer queue")
        producer_finished = time.monotonic()

    consumer_thread.join(timeout=max(0, deadline - time.monotonic()))
    if consumer_thread.is_alive():
        errors.append("consumer did not finish before timeout")
        consumer_thread.join(timeout=2)
    finished = time.monotonic()
    missing = workload.messages - sum(seen)
    result = {
        "system": backend,
        "topic": topic,
        "bootstrap": bootstrap,
        "version": version,
        "config": workload.__dict__,
        "produced": sent_count,
        "acked": counters["acked"],
        "consumed": counters["consumed"],
        "duplicates": counters["duplicates"],
        "invalid": counters["invalid"],
        "missing": missing,
        "errors": errors,
        "produce_elapsed_s": producer_finished - producer_started,
        "elapsed_s": finished - producer_started,
        "produce_rate_s": counters["acked"]
        / max(producer_finished - producer_started, 1e-9),
        "completion_rate_s": counters["consumed"]
        / max(finished - producer_started, 1e-9),
        "ack_latency": ack_latency.summary(),
        "delivery_latency": delivery_latency.summary(),
    }
    result["valid"] = (
        not errors
        and result["acked"] == workload.messages
        and result["consumed"] == workload.messages
        and not result["duplicates"]
        and not result["invalid"]
        and not missing
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--systems", nargs="+", choices=BACKENDS, default=list(BACKENDS)
    )
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=256)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--timeout-s", type=float, default=60)
    parser.add_argument("--work-ms", type=float, default=0)
    parser.add_argument(
        "--rate",
        type=float,
        default=0,
        help="offered messages/s; 0 sends as fast as possible",
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if (
        args.messages < 1
        or args.payload_bytes < HEADER.size
        or args.partitions < 1
        or args.timeout_s <= 0
        or args.work_ms < 0
        or args.rate < 0
    ):
        parser.error(
            "messages, partitions and timeout must be positive; payload must be at least 16 bytes; work and rate must be nonnegative"
        )
    workload = Workload(
        args.messages,
        args.payload_bytes,
        args.partitions,
        args.timeout_s,
        args.work_ms,
        args.rate,
    )
    output = (
        args.output
        or ROOT
        / "results"
        / f"custom-broker-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}"
        / "summary.json"
    )
    results = []
    for system in args.systems:
        print(f"[broker-bench] running {system}", flush=True)
        try:
            result = run_case(system, workload)
        except Exception as exc:
            result = {"system": system, "valid": False, "errors": [str(exc)]}
        results.append(result)
        print(
            f"[broker-bench] {system}: {result.get('consumed', 0)}/{workload.messages} consumed, valid={result['valid']}",
            flush=True,
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "host": {
                    "platform": platform.platform(),
                    "processor": platform.processor(),
                },
                "client": {
                    "confluent_kafka": confluent_kafka.version()[0],
                    "librdkafka": confluent_kafka.libversion()[0],
                },
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )
    print(f"[broker-bench] {output}")
    if not all(result["valid"] for result in results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
