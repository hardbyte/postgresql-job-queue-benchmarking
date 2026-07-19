"""Shared CDC source-write load generator.

One implementation for every system under test (design §6): rate-credit
pacing on real wall-clock elapsed time (the normative rule from
CONTRIBUTING_ADAPTERS.md), and the source-side ledger (highest seq per
(table, key) + tombstones + balances) dumped to JSON at shutdown for the
drain-verify step.

Modes (design §6):
- events: single-table insert/update/delete stream, one tx per batch.
- ledger: multi-table with a cross-table invariant — each source tx inserts
  one transfer and upserts both account balances (exactly 3 events/tx, so
  the receiver can track transaction integrity; SUM(balance) is conserved).
- outbox: the same ledger domain writes plus an outbox row in the same tx;
  the publication captures ONLY the outbox table. A janitor deletes
  delivered outbox rows (outbox bloat under churn is part of the cost).

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

EVENTS_TABLE = "cdc_bench.events"
ACCOUNTS_TABLE = "cdc_bench.accounts"
TRANSFERS_TABLE = "cdc_bench.transfers"
OUTBOX_TABLE = "cdc_bench.outbox"

ACCOUNT_INITIAL_BALANCE = 1000


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


class SourceLedger:
    """table -> pk -> [seq, balance|None, deleted]."""

    def __init__(self) -> None:
        self.tables: dict[str, dict[int, list]] = {}

    def upsert(self, table: str, pk: int, *, seq: int,
               balance: int | None = None) -> None:
        self.tables.setdefault(table, {})[pk] = [seq, balance, False]

    def get(self, table: str, pk: int) -> list | None:
        return self.tables.get(table, {}).get(pk)

    def delete(self, table: str, pk: int) -> None:
        self.tables[table][pk][2] = True

    def dump(self, path: str, totals: dict,
             only_tables: set[str] | None = None) -> None:
        """Dump the verification ledger. `only_tables` restricts to the
        published tables — outbox mode tracks domain-table state internally
        (balances) but only the outbox stream is replicated and verified."""
        with open(path, "w") as fh:
            json.dump(
                {
                    "totals": totals,
                    "tables": {
                        table: {str(pk): entry for pk, entry in entries.items()}
                        for table, entries in self.tables.items()
                        if only_tables is None or table in only_tables
                    },
                },
                fh,
            )


class Workload:
    """Base: one call to run_ops(n, tx_id, emitted_us) executes n paced ops
    in one or more source transactions and updates the ledger."""

    events_per_tx: int | None = None  # set when every tx has a fixed shape
    published_tables: set[str] | None = None  # None = dump everything

    def __init__(self, conn: psycopg.Connection, rng: random.Random,
                 ledger: SourceLedger, args: argparse.Namespace) -> None:
        self.conn = conn
        self.rng = rng
        self.ledger = ledger
        self.args = args
        self.tx_total = 0

    def setup(self) -> None:
        raise NotImplementedError

    def run_ops(self, n: int, next_tx_id, emitted_us: int) -> None:
        raise NotImplementedError

    def janitor_tick(self) -> None:  # outbox cleanup hook
        pass


class EventsWorkload(Workload):
    def __init__(self, *a) -> None:
        super().__init__(*a)
        self.keys = KeySpace(self.rng)
        self.next_pk = 1
        mix = [int(p) for p in self.args.op_mix.split("/")]
        assert sum(mix) == 100, "--op-mix must sum to 100"
        self.ins_pct, self.upd_pct, self.del_pct = mix
        self.payload = memoryview(self.rng.randbytes(self.args.payload_bytes))

    def setup(self) -> None:
        pass  # schema created by orchestrator preflight

    def run_ops(self, n: int, next_tx_id, emitted_us: int) -> None:
        tx_id = next_tx_id()
        inserts, updates, deletes = [], [], []
        for _ in range(n):
            roll = self.rng.randrange(100)
            can_mutate = len(self.keys.live) > 0
            at_capacity = len(self.keys.live) >= self.args.key_cardinality
            # Pick the op from the mix, then reconcile with keyspace bounds.
            # A bounded keyspace can't sustain the insert rate forever, so at
            # capacity an insert roll becomes a DELETE (not an update) — that
            # frees a slot and keeps tombstone churn flowing. The old code
            # folded capacity into the update branch, which made the delete
            # branch unreachable once the keyspace filled and silently zeroed
            # the configured delete percentage for the rest of the run.
            if not can_mutate:
                op = "insert"
            elif roll < self.ins_pct:
                op = "delete" if at_capacity else "insert"
            elif roll < self.ins_pct + self.upd_pct:
                op = "update"
            else:
                op = "delete"

            if op == "insert":
                pk = self.next_pk
                self.next_pk += 1
                self.keys.add(pk)
                self.ledger.upsert(EVENTS_TABLE, pk, seq=1)
                inserts.append((pk, 1, tx_id, self.payload, emitted_us))
            elif op == "update":
                pk = self.keys.pick()
                entry = self.ledger.get(EVENTS_TABLE, pk)
                entry[0] += 1
                updates.append((entry[0], tx_id, emitted_us, pk))
            else:
                pk = self.keys.pick()
                self.keys.remove(pk)
                self.ledger.delete(EVENTS_TABLE, pk)
                deletes.append(pk)
        with self.conn.cursor() as cur:
            if inserts:
                cur.executemany(
                    "INSERT INTO cdc_bench.events"
                    " (pk, seq, tx_id, payload, emitted_us)"
                    " VALUES (%s, %s, %s, %s, %s)", inserts)
            if updates:
                cur.executemany(
                    "UPDATE cdc_bench.events"
                    " SET seq = %s, tx_id = %s, emitted_us = %s"
                    " WHERE pk = %s", updates)
            if deletes:
                cur.execute(
                    "DELETE FROM cdc_bench.events WHERE pk = ANY(%s)",
                    (deletes,))
        self.conn.commit()
        self.tx_total += 1


class LedgerWorkload(Workload):
    """One op = one transfer tx: INSERT transfer + upsert both accounts.
    Exactly 3 replicated events per tx; SUM(balance) is conserved."""

    events_per_tx = 3

    def __init__(self, *a) -> None:
        super().__init__(*a)
        self.next_transfer = 1
        self.include_outbox = False

    def setup(self) -> None:
        pass

    def _account_write(self, cur, account_id: int, delta: int,
                       tx_id: int, emitted_us: int) -> None:
        entry = self.ledger.get(ACCOUNTS_TABLE, account_id)
        if entry is None:
            balance = ACCOUNT_INITIAL_BALANCE + delta
            self.ledger.upsert(ACCOUNTS_TABLE, account_id, seq=1, balance=balance)
            cur.execute(
                "INSERT INTO cdc_bench.accounts"
                " (pk, balance, seq, tx_id, emitted_us)"
                " VALUES (%s, %s, 1, %s, %s)",
                (account_id, balance, tx_id, emitted_us))
        else:
            entry[0] += 1
            entry[1] += delta
            cur.execute(
                "UPDATE cdc_bench.accounts"
                " SET balance = %s, seq = %s, tx_id = %s, emitted_us = %s"
                " WHERE pk = %s",
                (entry[1], entry[0], tx_id, emitted_us, account_id))

    def run_ops(self, n: int, next_tx_id, emitted_us: int) -> None:
        for _ in range(n):
            tx_id = next_tx_id()
            from_id = self.rng.randrange(1, self.args.key_cardinality + 1)
            to_id = self.rng.randrange(1, self.args.key_cardinality + 1)
            while to_id == from_id:  # 3 events per tx, always
                to_id = self.rng.randrange(1, self.args.key_cardinality + 1)
            amount = self.rng.randrange(1, 10)
            transfer_pk = self.next_transfer
            self.next_transfer += 1
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO cdc_bench.transfers"
                    " (pk, from_id, to_id, amount, seq, tx_id, emitted_us)"
                    " VALUES (%s, %s, %s, %s, 1, %s, %s)",
                    (transfer_pk, from_id, to_id, amount, tx_id, emitted_us))
                self._account_write(cur, from_id, -amount, tx_id, emitted_us)
                self._account_write(cur, to_id, amount, tx_id, emitted_us)
                if self.include_outbox:
                    self._outbox_write(cur, transfer_pk, from_id, to_id,
                                       amount, tx_id, emitted_us)
            self.conn.commit()
            self.tx_total += 1
            if not self.include_outbox:
                self.ledger.upsert(TRANSFERS_TABLE, transfer_pk, seq=1)

    def _outbox_write(self, cur, transfer_pk, from_id, to_id, amount,
                      tx_id, emitted_us) -> None:
        raise NotImplementedError


class OutboxWorkload(LedgerWorkload):
    """Ledger domain writes + an outbox row in the same tx. The publication
    captures ONLY the outbox, so the verified stream (and the ledger dump)
    is the outbox stream; domain tables exist to make the write
    amplification honest."""

    events_per_tx = None  # 1 replicated event per tx — no tx tracking needed
    published_tables = {OUTBOX_TABLE}

    def __init__(self, *a) -> None:
        super().__init__(*a)
        self.include_outbox = True
        self.last_janitor = time.monotonic()

    def _outbox_write(self, cur, transfer_pk, from_id, to_id, amount,
                      tx_id, emitted_us) -> None:
        payload = json.dumps({"transfer": transfer_pk, "from": from_id,
                              "to": to_id, "amount": amount})
        cur.execute(
            "INSERT INTO cdc_bench.outbox"
            " (pk, aggregate_id, event_type, payload, seq, tx_id, emitted_us)"
            " VALUES (%s, %s, 'transfer', %s, 1, %s, %s)",
            (transfer_pk, from_id, payload, tx_id, emitted_us))
        self.ledger.upsert(OUTBOX_TABLE, transfer_pk, seq=1)

    def janitor_tick(self) -> None:
        # Delete outbox rows older than the retention window; their delete
        # events ride the publication and the verifier checks tombstones.
        now = time.monotonic()
        if now - self.last_janitor < self.args.outbox_janitor_every_s:
            return
        self.last_janitor = now
        cutoff_us = time.time_ns() // 1000 - int(
            self.args.outbox_retention_s * 1_000_000)
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM cdc_bench.outbox WHERE emitted_us < %s"
                " RETURNING pk", (cutoff_us,))
            for (pk,) in cur.fetchall():
                self.ledger.delete(OUTBOX_TABLE, pk)
        self.conn.commit()


WORKLOADS = {"events": EventsWorkload, "ledger": LedgerWorkload,
             "outbox": OutboxWorkload}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc-loadgen")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--mode", default="events", choices=sorted(WORKLOADS))
    parser.add_argument("--rate", type=float, default=200.0,
                        help="source ops/s (events: rows; ledger/outbox: transfers)")
    parser.add_argument("--op-mix", default="70/25/5", help="insert/update/delete %%")
    parser.add_argument("--key-cardinality", type=int, default=5000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--batch-max", type=int, default=128)
    parser.add_argument("--batch-ms", type=float, default=25.0)
    parser.add_argument("--sample-every-s", type=float, default=5.0)
    parser.add_argument("--outbox-retention-s", type=float, default=10.0)
    parser.add_argument("--outbox-janitor-every-s", type=float, default=5.0)
    parser.add_argument("--preload", type=int, default=0,
                        help="ledger/outbox modes: N accounts assumed "
                             "preloaded (balance 1000, seq 1, tx_id 0)")
    parser.add_argument("--preload-only", action="store_true",
                        help="write the preload rows and exit (run before "
                             "the SUT starts so its snapshot covers them)")
    parser.add_argument("--ledger-out", required=True)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL required")

    stop = False

    def _on_sigterm(_sig, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _on_sigterm)
    signal.signal(signal.SIGINT, _on_sigterm)

    rng = random.Random(args.seed)
    ledger = SourceLedger()
    conn = psycopg.connect(args.database_url, autocommit=False)

    if args.preload_only:
        assert args.mode in ("ledger", "outbox") and args.preload > 0
        emitted_us = time.time_ns() // 1000
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cdc_bench.accounts"
                " (pk, balance, seq, tx_id, emitted_us)"
                " SELECT g, %s, 1, 0, %s FROM generate_series(1, %s) g",
                (ACCOUNT_INITIAL_BALANCE, emitted_us, args.preload))
        conn.commit()
        conn.close()
        _log(f"preloaded {args.preload} accounts")
        return 0

    workload = WORKLOADS[args.mode](conn, rng, ledger, args)
    workload.setup()
    if args.preload > 0 and args.mode in ("ledger", "outbox"):
        # Rows written by a prior --preload-only run. Seeding the in-memory
        # ledger is load-bearing in both modes: _account_write takes the
        # INSERT path for any account it doesn't know, so an unseeded
        # preloaded account means a PK conflict on first touch. In ledger
        # mode the seeded rows are also what drain-verify expects the SUT's
        # snapshot to deliver; in outbox mode the dump filter drops them
        # (only the outbox table is published).
        for account_id in range(1, args.preload + 1):
            ledger.upsert(ACCOUNTS_TABLE, account_id, seq=1,
                          balance=ACCOUNT_INITIAL_BALANCE)

    tx_counter = 0

    def next_tx_id() -> int:
        nonlocal tx_counter
        tx_counter += 1
        return tx_counter

    ops_total = 0
    rate_credit = 0.0
    last_credit_tick = time.monotonic()
    window_ops = 0
    window_tx_base = 0
    window_write_ms: list[float] = []
    next_sample = time.monotonic() + args.sample_every_s

    _log(f"starting: mode={args.mode} rate={args.rate}/s "
         f"keys<={args.key_cardinality} batch<={args.batch_max}")

    while not stop:
        now = time.monotonic()
        # Normative pacing: credit on real elapsed time, never on nominal
        # loop period (CONTRIBUTING_ADAPTERS.md "Producer pacing"). Credit
        # accumulates uncapped across stalls, so a source-side hiccup is
        # followed by a catch-up burst (bounded per iteration by batch_max):
        # open-loop offered load stays constant against wall time, matching
        # the queue bench's reference shape ("dispatch up to batch_max").
        rate_credit += args.rate * (now - last_credit_tick)
        last_credit_tick = now
        whole = min(int(rate_credit), args.batch_max)
        if whole > 0:
            rate_credit -= whole
            emitted_us = time.time_ns() // 1000
            t_write = time.monotonic()
            workload.run_ops(whole, next_tx_id, emitted_us)
            window_write_ms.append((time.monotonic() - t_write) * 1000.0)
            ops_total += whole
            window_ops += whole
        workload.janitor_tick()

        now = time.monotonic()
        if now >= next_sample:
            dt = args.sample_every_s
            base = {"kind": "loadgen", "subject_kind": "loadgen", "subject": "",
                    "instance_id": 0, "window_s": dt}
            _emit({**base, "metric": "source_write_rate", "value": window_ops / dt})
            _emit({**base, "metric": "source_tx_rate",
                   "value": (workload.tx_total - window_tx_base) / dt})
            _emit({**base, "metric": "source_ops_total", "value": float(ops_total),
                   "window_s": 0})
            if window_write_ms:
                ranked = sorted(window_write_ms)
                p99 = ranked[min(len(ranked) - 1, int(len(ranked) * 0.99))]
                _emit({**base, "metric": "source_write_p99_ms", "value": p99})
            window_ops = 0
            window_tx_base = workload.tx_total
            window_write_ms = []
            while next_sample <= now:
                next_sample += args.sample_every_s

        time.sleep(min(args.batch_ms / 1000.0, 0.05))

    conn.close()
    expected_events = sum(len(t) for t in ledger.tables.values())
    _log(f"stopping: ops_total={ops_total} txes={workload.tx_total} "
         f"tracked_keys={expected_events}")
    ledger.dump(args.ledger_out, only_tables=workload.published_tables, totals={
        "ops": ops_total,
        "txes": workload.tx_total,
        "mode": args.mode,
        "events_per_tx": workload.events_per_tx,
        "balance_sum_expected": (
            ACCOUNT_INITIAL_BALANCE
            * len(ledger.tables.get(ACCOUNTS_TABLE, {}))
            if args.mode == "ledger" else None
        ),
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())
