#!/usr/bin/env python3
"""Segmented rotation engine — spike adapter (awa #169 / #197 v0.7 exploration).

Tests whether a *cursor-allocator claim-ledger* is both pin-immune (flat dead
tuples under a held MVCC horizon) and fast (~800/s+) while keeping per-job
claim/ack semantics. See DESIGN.md.

Everything on the hot path is append-only (reclaimed by TRUNCATE on rotation,
which a foreign snapshot does not block) or a Postgres sequence (nextval is not
an MVCC heap tuple). The only in-place UPDATE is the rotation pointer (~1/s).

Speaks the benchmark subprocess contract: reads config from env, optionally
reads `ENQUEUE <n>` pacing tokens on stdin, emits a startup descriptor and
per-`SAMPLE_EVERY_S` JSON sample lines on stdout.
"""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from collections import deque

import psycopg

SLOT_COUNT = int(os.environ.get("SEG_SLOT_COUNT", "16"))
ROTATE_MS = int(os.environ.get("SEG_ROTATE_MS", "1000"))
QUEUE = "bench"


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, str(default)))
    except ValueError:
        return default


DATABASE_URL = os.environ["DATABASE_URL"]
PRODUCER_RATE = _env_int("PRODUCER_RATE", 800)
PRODUCER_MODE = os.environ.get("PRODUCER_MODE", "fixed")
PRODUCER_PACING = os.environ.get("PRODUCER_PACING", "harness")
PRODUCER_BATCH_MAX = _env_int("PRODUCER_BATCH_MAX", 128)
TARGET_DEPTH = _env_int("TARGET_DEPTH", 1000)
WORKER_COUNT = _env_int("WORKER_COUNT", 32)
PAYLOAD_BYTES = _env_int("JOB_PAYLOAD_BYTES", 256)
WORK_MS = _env_int("JOB_WORK_MS", 1)
SAMPLE_EVERY_S = _env_int("SAMPLE_EVERY_S", 5)
INSTANCE_ID = _env_int("BENCH_INSTANCE_ID", 0)

PAYLOAD = json.dumps({"p": "x" * max(0, PAYLOAD_BYTES - 10)})

# psycopg wants postgresql:// ; the harness hands us postgres://
CONNINFO = DATABASE_URL.replace("postgres://", "postgresql://", 1)


def _schema_sql() -> str:
    parts = [
        "CREATE SCHEMA IF NOT EXISTS seg",
        "CREATE SEQUENCE IF NOT EXISTS seg.enqueue_seq",
        "CREATE SEQUENCE IF NOT EXISTS seg.dispatch_seq",
        """CREATE TABLE IF NOT EXISTS seg.ring_state (
               singleton BOOLEAN PRIMARY KEY DEFAULT TRUE,
               current_slot INT NOT NULL,
               generation BIGINT NOT NULL,
               slot_count INT NOT NULL,
               CHECK (singleton)
           )""",
        # Append-only seq-range -> slot map (the claim allocator metadata).
        """CREATE TABLE IF NOT EXISTS seg.segments (
               generation BIGINT NOT NULL,
               slot INT NOT NULL,
               first_seq BIGINT NOT NULL,
               next_seq BIGINT NOT NULL,
               PRIMARY KEY (next_seq)
           )""",
        "CREATE INDEX IF NOT EXISTS seg_segments_next ON seg.segments (next_seq)",
    ]
    for s in range(SLOT_COUNT):
        parts.append(
            f"""CREATE TABLE IF NOT EXISTS seg.events_{s} (
                    seq BIGINT PRIMARY KEY,
                    generation BIGINT NOT NULL,
                    payload JSONB NOT NULL,
                    enqueued_at TIMESTAMPTZ NOT NULL
                )"""
        )
        parts.append(
            f"""CREATE TABLE IF NOT EXISTS seg.claims_{s} (
                    claim_seq BIGINT PRIMARY KEY,
                    claimed_at TIMESTAMPTZ NOT NULL
                )"""
        )
        parts.append(
            f"""CREATE TABLE IF NOT EXISTS seg.done_{s} (
                    claim_seq BIGINT PRIMARY KEY,
                    completed_at TIMESTAMPTZ NOT NULL
                )"""
        )
    return ";\n".join(parts) + ";"


async def _install_schema() -> None:
    async with await psycopg.AsyncConnection.connect(CONNINFO, autocommit=True) as conn:
        await conn.execute(_schema_sql())
        # Seed the ring pointer once.
        await conn.execute(
            """INSERT INTO seg.ring_state (singleton, current_slot, generation, slot_count)
               VALUES (TRUE, 0, 0, %s) ON CONFLICT (singleton) DO NOTHING""",
            (SLOT_COUNT,),
        )


def _now_iso() -> str:
    return (
        time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        + f".{int((time.time() % 1) * 1000):03d}Z"
    )


def _emit(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def _emit_sample(metric: str, value: float, window_s: float) -> None:
    _emit(
        {
            "t": _now_iso(),
            "system": "segmented",
            "kind": "adapter",
            "subject_kind": "adapter",
            "subject": "",
            "metric": metric,
            "value": value,
            "window_s": window_s,
            "instance_id": INSTANCE_ID,
        }
    )


class Counters:
    def __init__(self) -> None:
        self.enqueued = 0
        self.completed = 0
        self.pickup_ms: deque[tuple[float, float]] = deque(maxlen=32768)


def _percentiles(events: deque[tuple[float, float]], window_s: float, now: float):
    cutoff = now - window_s
    values = sorted(v for t, v in events if t >= cutoff)
    if not values:
        return 0.0, 0.0, 0.0
    n = len(values)

    def q(p: float) -> float:
        return round(values[min(n - 1, max(0, int(round(p * (n - 1)))))], 3)

    return q(0.50), q(0.95), q(0.99)


async def _current_ring(conn: psycopg.AsyncConnection) -> tuple[int, int]:
    cur = await conn.execute("SELECT current_slot, generation FROM seg.ring_state")
    row = await cur.fetchone()
    return int(row[0]), int(row[1])


async def producer(conn, counters: Counters, shutdown: asyncio.Event) -> None:
    """Append events at the offered rate. Honours harness `ENQUEUE <n>` tokens
    when PRODUCER_PACING=harness, else self-paces at PRODUCER_RATE."""
    loop = asyncio.get_running_loop()
    rate_credit = 0.0
    last = loop.time()
    reader = None
    if PRODUCER_PACING == "harness":
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while not shutdown.is_set():
        if reader is not None:
            try:
                line = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            if not line:
                await asyncio.sleep(0.05)
                continue
            text = line.decode().strip()
            if not text.startswith("ENQUEUE "):
                continue
            n = int(text.split()[1])
        else:
            now = loop.time()
            rate_credit += PRODUCER_RATE * (now - last)
            last = now
            n = min(PRODUCER_BATCH_MAX, int(rate_credit))
            if n <= 0:
                await asyncio.sleep(0.005)
                continue
            rate_credit -= n

        slot, gen = await _current_ring(conn)
        # Reserve a contiguous seq range, append events, record the segment.
        async with conn.cursor() as cur:
            await cur.execute(
                f"""WITH ins AS (
                        INSERT INTO seg.events_{slot} (seq, generation, payload, enqueued_at)
                        SELECT nextval('seg.enqueue_seq'), %s, %s::jsonb, clock_timestamp()
                        FROM generate_series(1, %s)
                        RETURNING seq
                    )
                    SELECT min(seq), max(seq) FROM ins""",
                (gen, PAYLOAD, n),
            )
            lo, hi = await cur.fetchone()
            await cur.execute(
                """INSERT INTO seg.segments (generation, slot, first_seq, next_seq)
                   VALUES (%s, %s, %s, %s) ON CONFLICT (next_seq) DO NOTHING""",
                (gen, slot, lo, hi + 1),
            )
        counters.enqueued += n


async def _resolve_event(conn, claim_seq: int, shutdown: asyncio.Event):
    """Return (slot, enqueued_at) for claim_seq, waiting for it to be committed.

    A bounded race lets the dispatch cursor advance a few seqs past the enqueue
    high-water, so the event for our reserved claim_seq may not be visible yet.
    claim_seq=K corresponds 1:1 to event seq=K, so the producer WILL commit it;
    we wait rather than discard the seq (which would lose the job)."""
    while not shutdown.is_set():
        async with conn.cursor() as cur:
            # Slot for this seq: indexed range short-circuit (the disjoint
            # contiguous segment whose next_seq first exceeds claim_seq).
            await cur.execute(
                "SELECT slot FROM seg.segments WHERE next_seq > %s ORDER BY next_seq ASC LIMIT 1",
                (claim_seq,),
            )
            seg = await cur.fetchone()
            if seg is not None:
                slot = int(seg[0])
                await cur.execute(
                    f"SELECT enqueued_at FROM seg.events_{slot} WHERE seq = %s",
                    (claim_seq,),
                )
                ev = await cur.fetchone()
                if ev is not None:
                    return slot, ev[0]
        await asyncio.sleep(0.002)  # not yet committed; wait for our event
    return None


async def worker(conn, counters: Counters, shutdown: asyncio.Event) -> None:
    work_s = WORK_MS / 1000.0
    errors = 0
    while not shutdown.is_set():
        try:
            await _worker_once(conn, counters, work_s, shutdown)
        except Exception as exc:
            errors += 1
            if errors <= 5:
                print(f"[segmented] worker error #{errors}: {exc!r}", file=sys.stderr)
            await asyncio.sleep(0.01)


async def _worker_once(conn, counters: Counters, work_s: float, shutdown: asyncio.Event) -> None:
    """Claim and complete one job, or return quickly if there is no work."""
    # Claim: advance the dispatch cursor only when work is available. The
    # nextval in the target list is evaluated only when WHERE passes, so the
    # cursor stays within ~worker_count of the enqueue high-water.
    async with conn.cursor() as cur:
        await cur.execute(
            """SELECT nextval('seg.dispatch_seq')
               WHERE COALESCE(pg_sequence_last_value('seg.dispatch_seq'), 0)
                   < COALESCE(pg_sequence_last_value('seg.enqueue_seq'), 0)"""
        )
        row = await cur.fetchone()
    if row is None:
        await asyncio.sleep(0.002)
        return
    claim_seq = int(row[0])

    resolved = await _resolve_event(conn, claim_seq, shutdown)
    if resolved is None:
        return  # shutting down before our event arrived
    slot, enqueued_at = resolved

    # Claims/done go into the EVENT's slot, so a slot's claims+done
    # correspond exactly to its events and prune can prove drain by count.
    await conn.execute(
        f"""INSERT INTO seg.claims_{slot} (claim_seq, claimed_at)
            VALUES (%s, clock_timestamp()) ON CONFLICT DO NOTHING""",
        (claim_seq,),
    )

    # pickup latency: enqueue -> claim.
    pickup_ms = max(0.0, (time.time() - enqueued_at.timestamp()) * 1000.0)
    counters.pickup_ms.append((asyncio.get_running_loop().time(), pickup_ms))

    if work_s:
        await asyncio.sleep(work_s)

    await conn.execute(
        f"""INSERT INTO seg.done_{slot} (claim_seq, completed_at)
            VALUES (%s, clock_timestamp()) ON CONFLICT DO NOTHING""",
        (claim_seq,),
    )
    counters.completed += 1


async def maintenance(conn, shutdown: asyncio.Event) -> None:
    """Rotate the active slot every ROTATE_MS and best-effort TRUNCATE cold
    slots. A slot is cold when the dispatch cursor has passed every event in it
    and every claim in it has a done row."""
    await conn.execute("SET lock_timeout = '200ms'")
    while not shutdown.is_set():
        await asyncio.sleep(ROTATE_MS / 1000.0)
        try:
            slot, _gen = await _current_ring(conn)
            nxt = (slot + 1) % SLOT_COUNT
            await conn.execute(
                "UPDATE seg.ring_state SET current_slot = %s, generation = generation + 1 WHERE singleton",
                (nxt,),
            )
            # Reclaim any sealed (non-current) slot whose events are fully
            # claimed and completed: count(events)==count(claims)==count(done).
            # TRUNCATE is O(1) and proceeds under a foreign pinned snapshot, so
            # this is the pin-immune reclamation path.
            for s in range(SLOT_COUNT):
                if s == nxt:
                    continue
                cur = await conn.execute(
                    f"""SELECT
                            (SELECT count(*) FROM seg.events_{s}),
                            (SELECT count(*) FROM seg.claims_{s}),
                            (SELECT count(*) FROM seg.done_{s})"""
                )
                n_events, n_claims, n_done = await cur.fetchone()
                if n_events > 0 and n_events == n_claims == n_done:
                    try:
                        await conn.execute(
                            f"TRUNCATE seg.events_{s}, seg.claims_{s}, seg.done_{s}"
                        )
                    except psycopg.errors.LockNotAvailable:
                        pass
        except Exception as exc:  # best-effort maintenance; never crash the run
            print(f"[segmented] maintenance: {exc!r}", file=sys.stderr)


async def sampler(conn, counters: Counters, shutdown: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    now_epoch = int(time.time())
    await asyncio.sleep(SAMPLE_EVERY_S - (now_epoch % SAMPLE_EVERY_S))
    last_enq, last_cmp, last_tick = 0, 0, loop.time()
    while not shutdown.is_set():
        deadline = loop.time() + SAMPLE_EVERY_S
        while not shutdown.is_set() and loop.time() < deadline:
            await asyncio.sleep(min(0.5, deadline - loop.time()))
        dt = max(0.001, loop.time() - last_tick)
        enq_rate = (counters.enqueued - last_enq) / dt
        cmp_rate = (counters.completed - last_cmp) / dt
        last_enq, last_cmp, last_tick = counters.enqueued, counters.completed, loop.time()
        p50, p95, p99 = _percentiles(counters.pickup_ms, 30.0, loop.time())

        depth = 0.0
        if INSTANCE_ID == 0:
            try:
                cur = await conn.execute(
                    """SELECT GREATEST(0, COALESCE(pg_sequence_last_value('seg.enqueue_seq'),0)
                                        - COALESCE(pg_sequence_last_value('seg.dispatch_seq'),0))"""
                )
                depth = float((await cur.fetchone())[0])
            except Exception:
                depth = 0.0

        _emit_sample("claim_p50_ms", p50, 30.0)
        _emit_sample("claim_p95_ms", p95, 30.0)
        _emit_sample("claim_p99_ms", p99, 30.0)
        _emit_sample("enqueue_rate", enq_rate, float(SAMPLE_EVERY_S))
        _emit_sample("completion_rate", cmp_rate, float(SAMPLE_EVERY_S))
        _emit_sample("queue_depth", depth, 0.0)
        if INSTANCE_ID == 0:
            _emit_sample("total_backlog", depth, 0.0)
            _emit_sample("producer_target_rate", float(PRODUCER_RATE), 0.0)


async def main() -> None:
    event_tables = (
        ["seg.ring_state", "seg.segments"]
        + [f"seg.events_{s}" for s in range(SLOT_COUNT)]
        + [f"seg.claims_{s}" for s in range(SLOT_COUNT)]
        + [f"seg.done_{s}" for s in range(SLOT_COUNT)]
    )
    _emit(
        {
            "kind": "descriptor",
            "system": "segmented",
            "event_tables": event_tables,
            "extensions": [],
            "version": "spike",
            "schema_version": "seg-1",
            "db_name": "segmented_bench",
            "started_at": _now_iso(),
            "instance_id": INSTANCE_ID,
        }
    )

    if INSTANCE_ID == 0:
        await _install_schema()

    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            signal.signal(sig, lambda *_a: shutdown.set())

    counters = Counters()
    conns: list[psycopg.AsyncConnection] = []

    async def _conn() -> psycopg.AsyncConnection:
        c = await psycopg.AsyncConnection.connect(CONNINFO, autocommit=True)
        conns.append(c)
        return c

    tasks = [
        asyncio.create_task(producer(await _conn(), counters, shutdown)),
        asyncio.create_task(maintenance(await _conn(), shutdown)),
        asyncio.create_task(sampler(await _conn(), counters, shutdown)),
    ]
    for _ in range(WORKER_COUNT):
        tasks.append(asyncio.create_task(worker(await _conn(), counters, shutdown)))

    await shutdown.wait()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    for c in conns:
        try:
            await c.close()
        except Exception:
            pass
    print("[segmented] long_horizon: shutdown signal received", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
