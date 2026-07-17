"""Shared CDC source-write load generator.

One implementation for every system under test (design §6): rate-credit
pacing on real wall-clock elapsed time (the normative rule from
CONTRIBUTING_ADAPTERS.md), an insert/update/delete mix over a bounded key
set, and the source-side ledger (highest seq per key + tombstones) dumped
to a JSON file at shutdown for the drain-verify step.

Runs as a subprocess of the CDC orchestrator: JSONL samples on stdout,
logs on stderr, SIGTERM → dump ledger and exit 0.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import sys
import time

import psycopg

SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS cdc_bench;
CREATE TABLE IF NOT EXISTS cdc_bench.events (
    pk         bigint PRIMARY KEY,
    seq        bigint NOT NULL,
    tx_id      bigint NOT NULL,
    payload    bytea  NOT NULL,
    emitted_us bigint NOT NULL
);
"""


def _emit(record: dict) -> None:
    sys.stdout.write(json.dumps(record) + "\n")
    sys.stdout.flush()


def _log(msg: str) -> None:
    print(f"[loadgen] {msg}", file=sys.stderr, flush=True)


class KeySpace:
    """Live keys with O(1) random pick and swap-pop removal."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.live: list[int] = []
        self.pos: dict[int, int] = {}

    def add(self, pk: int) -> None:
        self.pos[pk] = len(self.live)
        self.live.append(pk)

    def pick(self) -> int:
        return self.live[self.rng.randrange(len(self.live))]

    def remove(self, pk: int) -> None:
        idx = self.pos.pop(pk)
        last = self.live.pop()
        if idx < len(self.live):
            self.live[idx] = last
            self.pos[last] = idx


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-loadgen")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--rate", type=float, default=200.0, help="target source ops/s")
    parser.add_argument("--op-mix", default="70/25/5", help="insert/update/delete %%")
    parser.add_argument("--key-cardinality", type=int, default=5000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--batch-max", type=int, default=128)
    parser.add_argument("--batch-ms", type=float, default=25.0)
    parser.add_argument("--sample-every-s", type=float, default=5.0)
    parser.add_argument("--ledger-out", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    if not args.database_url:
        parser.error("--database-url or DATABASE_URL required")
    ins_pct, upd_pct, del_pct = (int(p) for p in args.op_mix.split("/"))
    if ins_pct + upd_pct + del_pct != 100:
        parser.error(f"--op-mix must sum to 100, got {args.op_mix}")

    stop = False

    def _on_sigterm(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)

    rng = random.Random(args.seed)
    keys = KeySpace(rng)
    # pk -> (seq, deleted). Deleted keys stay for the verifier's tombstone
    # check; pks are never reused.
    ledger: dict[int, list] = {}
    next_pk = 1
    tx_id = 0
    ops_total = 0
    tx_total = 0

    conn = psycopg.connect(args.database_url, autocommit=False)
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()

    payload = memoryview(rng.randbytes(args.payload_bytes))

    rate_credit = 0.0
    last_credit_tick = time.monotonic()
    window_ops = 0
    window_tx = 0
    window_write_ms: list[float] = []
    next_sample = time.monotonic() + args.sample_every_s

    _log(
        f"starting: rate={args.rate}/s mix={args.op_mix} "
        f"keys<={args.key_cardinality} batch<={args.batch_max}"
    )

    while not stop:
        now = time.monotonic()
        # Normative pacing: credit on real elapsed time, never on nominal
        # loop period (CONTRIBUTING_ADAPTERS.md "Producer pacing").
        rate_credit += args.rate * (now - last_credit_tick)
        last_credit_tick = now
        whole = min(int(rate_credit), args.batch_max)

        if whole > 0:
            rate_credit -= whole
            tx_id += 1
            emitted_us = time.time_ns() // 1000
            inserts: list[tuple] = []
            updates: list[tuple] = []
            deletes: list[int] = []
            for _ in range(whole):
                roll = rng.randrange(100)
                can_mutate = len(keys.live) > 0
                at_capacity = len(keys.live) >= args.key_cardinality
                if (roll < ins_pct and not at_capacity) or not can_mutate:
                    pk = next_pk
                    next_pk += 1
                    keys.add(pk)
                    ledger[pk] = [1, False]
                    inserts.append((pk, 1, tx_id, payload, emitted_us))
                elif roll < ins_pct + upd_pct or at_capacity:
                    pk = keys.pick()
                    entry = ledger[pk]
                    entry[0] += 1
                    updates.append((entry[0], tx_id, emitted_us, pk))
                else:
                    pk = keys.pick()
                    keys.remove(pk)
                    ledger[pk][1] = True
                    deletes.append(pk)
            t_write = time.monotonic()
            with conn.cursor() as cur:
                if inserts:
                    cur.executemany(
                        "INSERT INTO cdc_bench.events"
                        " (pk, seq, tx_id, payload, emitted_us)"
                        " VALUES (%s, %s, %s, %s, %s)",
                        inserts,
                    )
                if updates:
                    cur.executemany(
                        "UPDATE cdc_bench.events"
                        " SET seq = %s, tx_id = %s, emitted_us = %s"
                        " WHERE pk = %s",
                        updates,
                    )
                if deletes:
                    cur.execute(
                        "DELETE FROM cdc_bench.events WHERE pk = ANY(%s)",
                        (deletes,),
                    )
            conn.commit()
            window_write_ms.append((time.monotonic() - t_write) * 1000.0)
            ops_total += whole
            tx_total += 1
            window_ops += whole
            window_tx += 1

        now = time.monotonic()
        if now >= next_sample:
            dt = args.sample_every_s
            base = {"kind": "loadgen", "subject_kind": "loadgen", "subject": "",
                    "instance_id": 0, "window_s": dt}
            _emit({**base, "metric": "source_write_rate", "value": window_ops / dt})
            _emit({**base, "metric": "source_tx_rate", "value": window_tx / dt})
            _emit({**base, "metric": "source_ops_total", "value": float(ops_total),
                   "window_s": 0})
            if window_write_ms:
                ranked = sorted(window_write_ms)
                p99 = ranked[min(len(ranked) - 1, int(len(ranked) * 0.99))]
                _emit({**base, "metric": "source_write_p99_ms", "value": p99})
            window_ops = 0
            window_tx = 0
            window_write_ms = []
            while next_sample <= now:
                next_sample += args.sample_every_s

        # Sleep to the next batch tick without oversleeping a pending stop.
        time.sleep(min(args.batch_ms / 1000.0, 0.05))

    conn.close()
    live = sum(1 for _seq, deleted in ledger.values() if not deleted)
    _log(f"stopping: ops_total={ops_total} keys={len(ledger)} live={live}")
    with open(args.ledger_out, "w") as fh:
        json.dump(
            {
                "totals": {"ops": ops_total, "txes": tx_total, "keys": len(ledger)},
                "keys": {str(pk): {"seq": seq, "deleted": deleted}
                         for pk, (seq, deleted) in ledger.items()},
            },
            fh,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
