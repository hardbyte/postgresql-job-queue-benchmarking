"""CDC-suite orchestrator (docs/cdc-harness-design.md).

Thin driver reusing bench_harness building blocks (Sample, RawCsvWriter,
phase DSL, summary/manifest writers). Per run: preflight Postgres (database,
source schema, publication, stale-slot cleanup), launch receiver → adapter →
loadgen, walk the phase list applying consumer-level chaos through the
receiver control API, then stop the loadgen, wait for drain, and verify the
source ledger against every consumer's delivered state.

M1 scope: single system per run (`pgoutput-raw`), consumer chaos hooks
(consumer-dead / consumer-slow / sink-outage), slot metrics poller.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import secrets
import shlex
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from cdc_harness.adapters import ADAPTERS, LaunchCtx, ManagedProc

from bench_harness.phases import Phase, PhaseType, parse_phase_spec
from bench_harness.sample import Sample, now_iso
from bench_harness.writers import (
    RawCsvWriter,
    build_manifest,
    compute_summary,
    write_manifest,
    write_summary,
)

REPO_ROOT = Path(__file__).resolve().parent.parent
RECEIVER_CRATE = REPO_ROOT / "cdc-receiver"
DEFAULT_PG_URL = "postgres://bench:bench@localhost:15555"


def receiver_binary(build: bool = True) -> Path:
    """Resolve the receiver binary through cargo metadata — the target dir
    may be redirected globally (shared-target setups)."""
    meta = subprocess.run(
        ["cargo", "metadata", "--format-version", "1", "--no-deps",
         "--manifest-path", str(RECEIVER_CRATE / "Cargo.toml")],
        capture_output=True, text=True, check=True,
    )
    target_dir = Path(json.loads(meta.stdout)["target_directory"])
    binary = target_dir / "release" / "cdc-receiver"
    if not binary.exists() and build:
        log("building cdc-receiver (cargo build --release)…")
        subprocess.run(
            ["cargo", "build", "--release"], cwd=RECEIVER_CRATE, check=True
        )
    if not binary.exists():
        raise SystemExit(f"cdc-receiver binary not found at {binary}")
    return binary

CDC_SCENARIOS: dict[str, list[str]] = {
    # Fast end-to-end pipe check: steady state, one dead consumer, heal, drain.
    "smoke": [
        "warmup=warmup:5s",
        "clean_1=clean:20s",
        "dead=consumer-dead(id=1):15s",
        "heal=clean:10s",
        "drain=recovery:10s",
    ],
    "fanout_steady": [
        "warmup=warmup:10m",
        "clean_1=clean:60m",
        "drain=recovery:10m",
    ],
    "dead_consumer": [
        "warmup=warmup:10m",
        "clean_1=clean:20m",
        "dead=consumer-dead(id=2):30m",
        "heal=clean:20m",
        "drain=recovery:10m",
    ],
    "slow_consumer": [
        "warmup=warmup:10m",
        "clean_1=clean:20m",
        "slow=consumer-slow(id=2,latency=250):30m",
        "heal=clean:20m",
        "drain=recovery:10m",
    ],
    "sink_outage": [
        "warmup=warmup:10m",
        "clean_1=clean:20m",
        "outage=sink-outage:15m",
        "heal=clean:20m",
        "drain=recovery:30m",
    ],
    "big_transaction": [
        "warmup=warmup:5m",
        "clean_1=clean:10m",
        "bigtx=big-tx(rows=1000000):5m",
        "post=clean:10m",
        "drain=recovery:10m",
    ],
    "ddl_mid_stream": [
        "warmup=warmup:5m",
        "clean_1=clean:10m",
        "ddl=ddl-change:2m",
        "post=clean:10m",
        "drain=recovery:5m",
    ],
    # Cross-table snapshot consistency: run with --mode ledger
    # --preload 10000000 --snapshot-mode initial. Verifies the snapshot ↔
    # stream handoff loses/duplicates nothing while writes continue.
    # (pgoutput-raw has no snapshot support — expected FAIL on the baseline.)
    "snapshot_consistency": [
        "warmup=warmup:5m",
        "clean_1=clean:30m",
        "drain=recovery:15m",
    ],
    # Advanced/destructive: verify is EXPECTED to fail if invalidation hits;
    # the interesting output is slot_wal_status + the SUT's heal behaviour.
    "slot_invalidation": [
        "warmup=warmup:5m",
        "clean_1=clean:10m",
        "invalidate=slot-invalidation(keep=32MB):15m",
        "heal=clean:10m",
        "drain=recovery:10m",
    ],
    # Consistency cells (run with --mode ledger / --mode outbox).
    "tx_integrity": [
        "warmup=warmup:5m",
        "clean_1=clean:30m",
        "dead=consumer-dead(id=2):10m",
        "heal=clean:10m",
        "drain=recovery:10m",
    ],
    # outbox-vs-WAL: run once with --mode ledger and once with --mode
    # outbox at the same --rate; compare pg_wal_bytes_delta, e2e lag, and
    # outbox-table bloat between the two runs.
    "outbox_vs_wal": [
        "warmup=warmup:5m",
        "clean_1=clean:30m",
        "drain=recovery:10m",
    ],
    # Fast M3 pipe check: big-tx spill + DDL survival at smoke scale.
    "smoke_m3": [
        "warmup=warmup:5s",
        "clean_1=clean:15s",
        "bigtx=big-tx(rows=200000):15s",
        "ddl=ddl-change:10s",
        "post=clean:10s",
        "drain=recovery:10s",
    ],
}


def log(msg: str) -> None:
    print(f"[cdc] {msg}", flush=True)


class PhaseTracker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._label = "setup"
        self._type = "setup"

    def set(self, label: str, type_: str) -> None:
        with self._lock:
            self._label, self._type = label, type_

    def get(self) -> tuple[str, str]:
        with self._lock:
            return self._label, self._type


class SampleSink:
    """Thread-safe raw.csv writer stamping run/phase/elapsed context."""

    def __init__(self, path: Path, run_id: str, system: str,
                 tracker: PhaseTracker) -> None:
        self._writer = RawCsvWriter(path)
        self._lock = threading.Lock()
        self.run_id = run_id
        self.system = system
        self.tracker = tracker
        self.t0 = time.monotonic()

    def write(self, *, subject_kind: str, subject: str, metric: str,
              value: float, window_s: float, instance_id: int = 0) -> None:
        label, type_ = self.tracker.get()
        sample = Sample(
            run_id=self.run_id,
            system=self.system,
            instance_id=instance_id,
            elapsed_s=round(time.monotonic() - self.t0, 3),
            sampled_at=now_iso(),
            phase_label=label,
            phase_type=type_,
            subject_kind=subject_kind,
            subject=subject,
            metric=metric,
            value=float(value),
            window_s=float(window_s),
        )
        with self._lock:
            self._writer.write(sample)

    def flush_close(self) -> None:
        with self._lock:
            self._writer.close()


def tail_jsonl(proc: subprocess.Popen, sink: SampleSink, name: str,
               on_descriptor=None) -> threading.Thread:
    """Tail a child's stdout: JSONL records → raw.csv samples."""

    def run() -> None:
        assert proc.stdout is not None
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                log(f"{name}: unparseable stdout line: {line[:200]}")
                continue
            if rec.get("kind") == "descriptor":
                if on_descriptor:
                    on_descriptor(rec)
                continue
            if "metric" not in rec:
                continue
            sink.write(
                subject_kind=rec.get("subject_kind", name),
                subject=rec.get("subject", ""),
                metric=rec["metric"],
                value=rec["value"],
                window_s=rec.get("window_s", 0.0),
                instance_id=int(rec.get("instance_id", 0)),
            )

    thread = threading.Thread(target=run, name=f"tail-{name}", daemon=True)
    thread.start()
    return thread


def http_json(url: str, payload: dict | None = None, timeout: float = 10.0):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"} if data else {},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read()
        try:
            return json.loads(body) if body else None
        except json.JSONDecodeError:
            return None  # plain-text endpoints ("ok")


# ── Preflight ────────────────────────────────────────────────────────────


def ensure_postgres(compose: bool) -> None:
    if not compose:
        return
    log("starting postgres (docker compose + cdc overlay)…")
    subprocess.run(
        ["docker", "compose", "-f", "docker-compose.yml",
         "-f", "docker-compose.cdc.yml", "up", "-d", "--wait", "postgres"],
        cwd=REPO_ROOT, check=True,
    )


_EVENTS_DDL = """CREATE TABLE cdc_bench.events (
    pk bigint PRIMARY KEY, seq bigint NOT NULL, tx_id bigint NOT NULL,
    payload bytea NOT NULL, emitted_us bigint NOT NULL)"""
_ACCOUNTS_DDL = """CREATE TABLE cdc_bench.accounts (
    pk bigint PRIMARY KEY, balance bigint NOT NULL, seq bigint NOT NULL,
    tx_id bigint NOT NULL, emitted_us bigint NOT NULL)"""
_TRANSFERS_DDL = """CREATE TABLE cdc_bench.transfers (
    pk bigint PRIMARY KEY, from_id bigint NOT NULL, to_id bigint NOT NULL,
    amount bigint NOT NULL, seq bigint NOT NULL, tx_id bigint NOT NULL,
    emitted_us bigint NOT NULL)"""
_OUTBOX_DDL = """CREATE TABLE cdc_bench.outbox (
    pk bigint PRIMARY KEY, aggregate_id bigint NOT NULL, event_type text NOT NULL,
    payload jsonb NOT NULL, seq bigint NOT NULL, tx_id bigint NOT NULL,
    emitted_us bigint NOT NULL)"""

MODE_SCHEMAS = {
    "events": [_EVENTS_DDL],
    "ledger": [_ACCOUNTS_DDL, _TRANSFERS_DDL],
    # Outbox mode: domain tables exist (write amplification is real) but
    # only the outbox is published.
    "outbox": [_ACCOUNTS_DDL, _TRANSFERS_DDL, _OUTBOX_DDL],
}
MODE_PUBLISHED_TABLES = {
    "events": ["cdc_bench.events"],
    "ledger": ["cdc_bench.accounts", "cdc_bench.transfers"],
    "outbox": ["cdc_bench.outbox"],
}
# Fixed replicated-events-per-tx shape, for the receiver's torn-tx tracking.
MODE_TX_EVENTS = {"events": 0, "ledger": 3, "outbox": 0}


def preflight(admin_url: str, db_name: str, slot_prefix: str,
              extra_databases: tuple[str, ...] = (),
              precreate_slots: tuple[str, ...] = (),
              mode: str = "events") -> str:
    """Create the bench DB (+ SUT extra DBs) + source schema + publication;
    drop stale slots. Returns the per-system DATABASE_URL."""
    with psycopg.connect(f"{admin_url}/postgres", autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW wal_level")
            (wal_level,) = cur.fetchone()
            if wal_level != "logical":
                raise SystemExit(
                    f"wal_level={wal_level!r}; CDC needs logical. Start PG with "
                    "docker compose -f docker-compose.yml -f docker-compose.cdc.yml "
                    "up -d postgres"
                )
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s", (db_name,)
            )
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{db_name}"')
            for name in extra_databases:
                # SUT state stores are recreated fresh each run — stale
                # state (e.g. Sequin config encrypted under an old key)
                # must not leak across runs.
                cur.execute(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
                cur.execute(f'CREATE DATABASE "{name}"')
    db_url = f"{admin_url}/{db_name}"
    with psycopg.connect(db_url, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Idempotent reruns: fresh schema, fresh publication, no stale
            # slots pinning WAL from a previous run. Bench databases all end
            # in _bench and slots are cluster-visible, so clean up leftovers
            # from every system — a prior system's stale slots otherwise eat
            # max_replication_slots headroom (etl needs ~2 per pipeline and
            # fails its table-sync quietly when the cluster runs out).
            cur.execute(
                "SELECT slot_name FROM pg_replication_slots"
                " WHERE database LIKE '%_bench' AND NOT active"
            )
            for (slot,) in cur.fetchall():
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot,))
            cur.execute("DROP SCHEMA IF EXISTS cdc_bench CASCADE")
            cur.execute("CREATE SCHEMA cdc_bench")
            for ddl in MODE_SCHEMAS[mode]:
                cur.execute(ddl)
            cur.execute("DROP PUBLICATION IF EXISTS cdc_pub")
            cur.execute(
                "CREATE PUBLICATION cdc_pub FOR TABLE "
                + ", ".join(MODE_PUBLISHED_TABLES[mode])
            )
            for slot in precreate_slots:
                cur.execute(
                    "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (slot,),
                )
    return db_url


def wait_for_slots(db_url: str, slot_names: list[str],
                   procs: list[ManagedProc], *, timeout_s: float,
                   logs_dir: Path, min_count: int = 0) -> None:
    """Uniform adapter readiness: every declared replication slot exists.
    SUTs with harness-opaque slot names declare none and pass min_count
    instead (preflight dropped all slots on the bench DB, so a bare count
    is unambiguous)."""
    deadline = time.monotonic() + timeout_s
    with psycopg.connect(db_url, autocommit=True) as conn:
        while True:
            for managed in procs:
                if managed.proc.poll() is not None:
                    raise SystemExit(
                        f"{managed.name} exited during startup "
                        f"(rc={managed.proc.returncode}); see {logs_dir}"
                    )
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT slot_name FROM pg_replication_slots"
                    " WHERE database = current_database()"
                )
                present = {row[0] for row in cur.fetchall()}
            missing = [s for s in slot_names if s not in present]
            if slot_names:
                if not missing:
                    return
            elif len(present) >= min_count:
                return
            if time.monotonic() > deadline:
                raise SystemExit(
                    f"adapter not ready after {timeout_s:.0f}s; "
                    f"missing slots: {missing} (see {logs_dir})"
                )
            time.sleep(1.0)


# ── Slot metrics poller ──────────────────────────────────────────────────


class SlotPoller(threading.Thread):
    def __init__(self, db_url: str, sink: SampleSink, every_s: float) -> None:
        super().__init__(name="slot-poller", daemon=True)
        self.db_url = db_url
        self.sink = sink
        self.every_s = every_s
        self.stop_event = threading.Event()

    def run(self) -> None:
        conn = psycopg.connect(self.db_url, autocommit=True)
        while not self.stop_event.wait(self.every_s):
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT s.slot_name,
                               pg_current_wal_lsn() - s.restart_lsn,
                               pg_current_wal_lsn() - s.confirmed_flush_lsn,
                               COALESCE(ss.spill_bytes, 0),
                               COALESCE(ss.spill_txns, 0)
                        FROM pg_replication_slots s
                        LEFT JOIN pg_stat_replication_slots ss
                               ON ss.slot_name = s.slot_name
                        WHERE s.database = current_database()
                    """)
                    rows = cur.fetchall()
            except psycopg.Error as exc:
                log(f"slot-poller: query failed: {exc}")
                continue
            for slot, retained, flush_lag, spill_bytes, spill_txns in rows:
                for metric, value in [
                    ("slot_retained_wal_bytes", retained),
                    ("slot_confirmed_flush_lag_bytes", flush_lag),
                    ("decode_spill_bytes", spill_bytes),
                    ("decode_spill_txns", spill_txns),
                ]:
                    if value is None:
                        continue
                    self.sink.write(
                        subject_kind="slot", subject=slot,
                        metric=metric, value=float(value), window_s=0.0,
                    )
        conn.close()


_MEM_UNITS = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}


def _parse_mem(text: str) -> float | None:
    # "123.4MiB / 7.629GiB" -> bytes of the usage part. Match the longest
    # unit first: every KiB/MiB/GiB also ends in "B", so a "B"-first scan
    # would strip only the trailing B and choke on "123.4Mi".
    usage = text.split("/")[0].strip()
    for unit, mult in sorted(_MEM_UNITS.items(), key=lambda kv: -len(kv[0])):
        if usage.endswith(unit):
            try:
                return float(usage[: -len(unit)]) * mult
            except ValueError:
                return None
    return None


class ResourceSampler(threading.Thread):
    """CPU/RSS per SUT process: docker stats for containers, /proc for
    native processes. Insulation layers get priced, not hidden (design §7)."""

    def __init__(self, sink: SampleSink, procs: list[ManagedProc],
                 every_s: float = 10.0) -> None:
        super().__init__(name="resource-sampler", daemon=True)
        self.sink = sink
        self.containers = [m.container for m in procs if m.container]
        self.native = [(m.name, m.proc.pid) for m in procs if not m.container]
        self.every_s = every_s
        self.stop_event = threading.Event()
        self._clk = os.sysconf("SC_CLK_TCK")
        self._prev_cpu: dict[str, tuple[float, float]] = {}

    def _sample_containers(self) -> None:
        if not self.containers:
            return
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}", *self.containers],
            capture_output=True, text=True,
        )
        for line in out.stdout.splitlines():
            try:
                name, cpu, mem = line.split("\t")
                cpu_pct = float(cpu.rstrip("%"))
            except ValueError:
                continue
            self.sink.write(subject_kind="container", subject=name,
                            metric="cpu_pct", value=cpu_pct, window_s=0)
            rss = _parse_mem(mem)
            if rss is not None:
                self.sink.write(subject_kind="container", subject=name,
                                metric="rss_bytes", value=rss, window_s=0)

    def _sample_native(self) -> None:
        for name, pid in self.native:
            try:
                statm = Path(f"/proc/{pid}/statm").read_text().split()
                stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
            except (OSError, IndexError):
                continue
            rss = int(statm[1]) * os.sysconf("SC_PAGE_SIZE")
            cpu_s = (int(stat[11]) + int(stat[12])) / self._clk  # utime+stime
            now = time.monotonic()
            prev = self._prev_cpu.get(name)
            self._prev_cpu[name] = (now, cpu_s)
            self.sink.write(subject_kind="container", subject=name,
                            metric="rss_bytes", value=float(rss), window_s=0)
            if prev is not None and now > prev[0]:
                pct = 100.0 * (cpu_s - prev[1]) / (now - prev[0])
                self.sink.write(subject_kind="container", subject=name,
                                metric="cpu_pct", value=pct, window_s=0)

    def run(self) -> None:
        while not self.stop_event.wait(self.every_s):
            try:
                self._sample_containers()
                self._sample_native()
            except Exception as exc:  # sampling must never kill the run
                log(f"resource-sampler: {exc}")


# ── Phase hooks (consumer chaos via receiver control API) ────────────────


def apply_phase_enter(phase: Phase, control_url: str, consumer_count: int,
                      db_url: str, mode: str) -> None:
    if phase.type is PhaseType.BIG_TX:
        # One huge transaction into an UNPUBLISHED ballast table: the
        # decode reorder buffer still processes (and spills) the whole tx
        # even though the publication filters it out — decode_spill_bytes
        # from the slot poller shows each system's cost.
        rows = phase.int_param("rows", 1_000_000)
        log(f"phase {phase.label}: writing {rows}-row single transaction (ballast)")
        with psycopg.connect(db_url) as conn:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS cdc_bench.ballast"
                            " (id bigint, filler text)")
                cur.execute(
                    "INSERT INTO cdc_bench.ballast"
                    " SELECT g, repeat('x', 100) FROM generate_series(1, %s) g",
                    (rows,),
                )
            conn.commit()
        log(f"phase {phase.label}: ballast transaction committed")
    elif phase.type is PhaseType.DDL_CHANGE:
        # DDL mid-stream on a *published* table is what stresses the
        # decoder; each mode publishes a different table set.
        table = MODE_PUBLISHED_TABLES[mode][0]
        column = f"extra_{phase.label}"
        log(f"phase {phase.label}: ALTER TABLE {table} ADD COLUMN {column}")
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} text")
    elif phase.type is PhaseType.SLOT_INVALIDATION:
        # Destructive by design: cap slot-retained WAL, take every consumer
        # down, and let the write load blow past the cap. slot_wal_status
        # (metrics poller) shows the walk to 'lost'; how the SUT reacts on
        # heal — detect? resurrect? silently lose? — is the result. The
        # run's verify step is EXPECTED to fail if the slot was invalidated.
        keep = phase.param("keep", "32MB") or "32MB"
        # `keep` is interpolated into ALTER SYSTEM below — accept only a
        # plain Postgres memory quantity.
        if not re.fullmatch(r"\d+(kB|MB|GB|TB)?", keep):
            raise SystemExit(
                f"phase {phase.label}: bad keep={keep!r} "
                "(expected e.g. 32MB, 1GB)"
            )
        log(f"phase {phase.label}: max_slot_wal_keep_size={keep}, all consumers dead")
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER SYSTEM SET max_slot_wal_keep_size = '{keep}'"
                )
                cur.execute("SELECT pg_reload_conf()")
        for cid in range(consumer_count):
            http_json(control_url, {"consumer_id": cid, "mode": "dead"})
    elif phase.type is PhaseType.CONSUMER_DEAD:
        cid = phase.int_param("id", 0)
        http_json(control_url, {"consumer_id": cid, "mode": "dead"})
        log(f"phase {phase.label}: consumer {cid} -> dead")
    elif phase.type is PhaseType.CONSUMER_SLOW:
        cid = phase.int_param("id", 0)
        latency = phase.int_param("latency", 250)
        http_json(control_url,
                  {"consumer_id": cid, "mode": "slow", "latency_ms": latency})
        log(f"phase {phase.label}: consumer {cid} -> slow ({latency}ms)")
    elif phase.type is PhaseType.SINK_OUTAGE:
        for cid in range(consumer_count):
            http_json(control_url, {"consumer_id": cid, "mode": "dead"})
        log(f"phase {phase.label}: all {consumer_count} consumers -> dead")


def apply_phase_exit(phase: Phase, control_url: str, consumer_count: int,
                     db_url: str) -> None:
    if phase.type is PhaseType.SLOT_INVALIDATION:
        with psycopg.connect(db_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER SYSTEM RESET max_slot_wal_keep_size")
                cur.execute("SELECT pg_reload_conf()")
        for cid in range(consumer_count):
            http_json(control_url, {"consumer_id": cid, "mode": "ok"})
        log(f"phase {phase.label}: keep-size reset, consumers -> ok")
    elif phase.type in (PhaseType.CONSUMER_DEAD, PhaseType.CONSUMER_SLOW):
        cid = phase.int_param("id", 0)
        http_json(control_url, {"consumer_id": cid, "mode": "ok"})
        log(f"phase {phase.label}: consumer {cid} -> ok")
    elif phase.type is PhaseType.SINK_OUTAGE:
        for cid in range(consumer_count):
            http_json(control_url, {"consumer_id": cid, "mode": "ok"})
        log(f"phase {phase.label}: all consumers -> ok")


# ── Drain verification ───────────────────────────────────────────────────


def verify_consumer(source: dict, receiver: dict) -> dict:
    """Compare the loadgen ledger against one consumer's delivered state.

    Source ledger: tables -> pk -> [seq, balance|None, deleted].
    Receiver:      tables -> pk -> {seq, balance, deleted}.
    """
    got_tables = receiver.get("tables", {})
    lost_events = 0
    lost_keys = 0
    missed_deletes = 0
    balance_mismatches = 0
    for table, entries in source["tables"].items():
        got = got_tables.get(table, {})
        for pk_str, (seq, balance, deleted) in entries.items():
            state = got.get(pk_str)
            if deleted:
                if state is None or not state.get("deleted"):
                    missed_deletes += 1
                continue
            if state is None or state.get("deleted"):
                lost_keys += 1
                lost_events += seq
                continue
            if state["seq"] < seq:
                lost_keys += 1
                lost_events += seq - state["seq"]
            elif balance is not None and state.get("balance") != balance:
                # Same seq but wrong balance = corrupted/misordered apply.
                balance_mismatches += 1
    torn_txs_open = int(receiver.get("open_txs", 0))
    return {
        "profile": receiver.get("profile"),
        "delivered": receiver.get("delivered"),
        "dups": receiver.get("dups"),
        "order_violations": receiver.get("order_violations"),
        "txs_completed": receiver.get("txs_completed"),
        "torn_txs_open": torn_txs_open,
        "lost_keys": lost_keys,
        "lost_events": lost_events,
        "missed_deletes": missed_deletes,
        "balance_mismatches": balance_mismatches,
        "pass": (lost_keys == 0 and lost_events == 0 and missed_deletes == 0
                 and balance_mismatches == 0 and torn_txs_open == 0),
    }


def drain_and_verify(receiver_base: str, consumer_count: int,
                     ledger_path: Path, timeout_s: float) -> dict:
    source = json.loads(ledger_path.read_text())
    deadline = time.monotonic() + timeout_s
    results: dict = {}
    while True:
        results = {}
        all_pass = True
        for cid in range(consumer_count):
            state = http_json(f"{receiver_base}/ledger/{cid}") or {}
            results[str(cid)] = verify_consumer(source, state)
            all_pass = all_pass and results[str(cid)]["pass"]
        if all_pass or time.monotonic() > deadline:
            break
        time.sleep(2.0)
    return {
        "source_totals": source["totals"],
        "consumers": results,
        "pass": all(r["pass"] for r in results.values()),
    }


# ── Main drive ───────────────────────────────────────────────────────────


def resolve_cdc_phases(scenario: str | None, phase_specs: list[str]) -> list[Phase]:
    specs = list(CDC_SCENARIOS[scenario]) if scenario else []
    specs.extend(phase_specs or [])
    if not specs:
        raise SystemExit("no phases: use --scenario or --phase")
    phases = [parse_phase_spec(s) for s in specs]
    if phases[0].type is not PhaseType.WARMUP:
        raise SystemExit("first phase must be warmup")
    return phases


def terminate(proc: subprocess.Popen | None, name: str, grace_s: float = 10.0) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=grace_s)
    except subprocess.TimeoutExpired:
        log(f"{name}: SIGKILL after {grace_s}s grace")
        proc.kill()
        proc.wait(timeout=5)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cdc", description=__doc__)
    parser.add_argument("--system", default="pgoutput-raw",
                        choices=sorted(ADAPTERS))
    parser.add_argument("--scenario", choices=sorted(CDC_SCENARIOS))
    parser.add_argument("--phase", action="append", default=[],
                        help="label=type:duration (repeatable)")
    parser.add_argument("--profiles", default="1xfast,2xnormal,1xslow",
                        help="consumer set, e.g. 2xfast,4xnormal,2xslow")
    parser.add_argument("--mode", default="events", choices=sorted(MODE_SCHEMAS),
                        help="workload shape: events | ledger | outbox")
    parser.add_argument("--preload", type=int, default=0,
                        help="ledger/outbox modes: preload N accounts before "
                             "the SUT starts (pair with --snapshot-mode initial)")
    parser.add_argument("--snapshot-mode", default="never",
                        choices=["never", "initial"])
    parser.add_argument("--rate", type=float, default=200.0)
    parser.add_argument("--op-mix", default="70/25/5")
    parser.add_argument("--key-cardinality", type=int, default=5000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--sample-every-s", type=float, default=5.0)
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--admin-url", default=DEFAULT_PG_URL)
    parser.add_argument("--results-root", default=str(REPO_ROOT / "results"))
    parser.add_argument("--drain-timeout-s", type=float, default=60.0)
    parser.add_argument("--adapter-ready-timeout-s", type=float, default=180.0)
    parser.add_argument("--skip-pg-setup", action="store_true",
                        help="assume postgres is already up with wal_level=logical")
    args = parser.parse_args(argv)

    phases = resolve_cdc_phases(args.scenario, args.phase)
    consumer_count = sum(
        int(p.split("x")[0]) if "x" in p else 1
        for p in args.profiles.split(",") if p
    )
    run_id = (
        f"cdc-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"
    )
    out_dir = Path(args.results_root) / run_id
    logs_dir = out_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log(f"run {run_id}: system={args.system} consumers={consumer_count} "
        f"({args.profiles}) rate={args.rate}/s")
    log(f"phases: {' → '.join(p.describe() for p in phases)}")

    entry = ADAPTERS[args.system]
    effective_snapshot = entry.effective_snapshot_mode(entry, args.snapshot_mode)
    if effective_snapshot != args.snapshot_mode:
        log(f"note: {entry.system} cannot honor --snapshot-mode "
            f"{args.snapshot_mode}; effective mode is {effective_snapshot!r} "
            "(recorded in manifest)")
    receiver_bin = receiver_binary()
    entry.prepare(entry)
    ensure_postgres(compose=not args.skip_pg_setup)
    db_url = preflight(args.admin_url, entry.db_name, entry.slot_prefix,
                       entry.extra_databases, entry.precreate_slots,
                       mode=args.mode)
    admin_parts = urllib.parse.urlsplit(args.admin_url)

    tracker = PhaseTracker()
    sink = SampleSink(out_dir / "raw.csv", run_id, args.system, tracker)
    receiver_base = f"http://127.0.0.1:{args.port}"
    control_url = f"{receiver_base}/control"
    ledger_path = out_dir / "ledger.json"

    receiver = loadgen = None
    adapter_procs: list[ManagedProc] = []
    poller = None
    resources = None
    # Per-instance so multi-process adapters can't interleave partial
    # descriptors into one dict; merged into the manifest at the end.
    instance_descriptors: dict[str, dict] = {}
    expected_slots: list[str] = []
    verdict: dict = {"pass": False, "error": "run did not reach verification"}
    exit_code = 1
    try:
        receiver = subprocess.Popen(
            [str(receiver_bin)],
            env={**os.environ,
                 "CDC_PORT": str(args.port),
                 "CONSUMER_PROFILES": args.profiles,
                 "ENVELOPE": entry.envelope,
                 "TX_EVENTS": str(MODE_TX_EVENTS[args.mode]),
                 "SAMPLE_EVERY_S": str(args.sample_every_s)},
            stdout=subprocess.PIPE,
            stderr=(logs_dir / "receiver.stderr.log").open("w"),
            text=True, cwd=REPO_ROOT,
        )
        tail_jsonl(receiver, sink, "receiver")
        for _ in range(50):
            try:
                http_json(f"{receiver_base}/healthz")
                break
            except (urllib.error.URLError, OSError):
                time.sleep(0.2)
        else:
            raise SystemExit("receiver did not become healthy")
        log("receiver healthy")

        if args.preload > 0:
            log(f"preloading {args.preload} accounts before SUT launch…")
            subprocess.run(
                [sys.executable, "-m", "cdc_harness.loadgen",
                 "--database-url", db_url, "--mode", args.mode,
                 "--preload", str(args.preload), "--preload-only",
                 "--ledger-out", "/dev/null"],
                cwd=REPO_ROOT, check=True,
            )

        ctx = LaunchCtx(
            source_tables=MODE_PUBLISHED_TABLES[args.mode],
            snapshot_mode=args.snapshot_mode,
            db_url=db_url,
            db_host=admin_parts.hostname or "127.0.0.1",
            db_port=admin_parts.port or 5432,
            db_user=admin_parts.username or "bench",
            db_password=admin_parts.password or "bench",
            db_name=entry.db_name,
            receiver_base=receiver_base,
            consumer_count=consumer_count,
            logs_dir=logs_dir,
            env=dict(os.environ),
        )
        adapter_procs = entry.launch(entry, ctx)
        for managed in adapter_procs:
            if managed.proc.stdout is not None:
                def _store_descriptor(rec: dict, name: str = managed.name) -> None:
                    instance_descriptors[name] = rec

                tail_jsonl(managed.proc, sink, managed.name,
                           on_descriptor=_store_descriptor)

        expected_slots = entry.slot_names(consumer_count)
        wait_for_slots(db_url, expected_slots, adapter_procs,
                       timeout_s=args.adapter_ready_timeout_s, logs_dir=logs_dir,
                       min_count=consumer_count)
        log(f"adapter up: slots={expected_slots or f'{consumer_count} (names SUT-derived)'}")

        loadgen = subprocess.Popen(
            [sys.executable, "-m", "cdc_harness.loadgen",
             "--database-url", db_url,
             "--mode", args.mode,
             "--rate", str(args.rate),
             "--op-mix", args.op_mix,
             "--key-cardinality", str(args.key_cardinality),
             "--preload", str(args.preload),
             "--payload-bytes", str(args.payload_bytes),
             "--sample-every-s", str(args.sample_every_s),
             "--ledger-out", str(ledger_path)],
            stdout=subprocess.PIPE,
            stderr=(logs_dir / "loadgen.stderr.log").open("w"),
            text=True, cwd=REPO_ROOT,
        )
        tail_jsonl(loadgen, sink, "loadgen")

        poller = SlotPoller(db_url, sink, args.sample_every_s)
        poller.start()
        resources = ResourceSampler(sink, adapter_procs)
        resources.start()

        for phase in phases:
            tracker.set(phase.label, phase.type.value)
            log(f"phase {phase.label} ({phase.type.value}) for {phase.duration_s}s")
            apply_phase_enter(phase, control_url, consumer_count, db_url,
                              args.mode)
            deadline = time.monotonic() + phase.duration_s
            watched = [(receiver, "receiver"), (loadgen, "loadgen")] + [
                (m.proc, m.name) for m in adapter_procs
            ]
            while time.monotonic() < deadline:
                for proc, name in watched:
                    if proc.poll() is not None:
                        raise SystemExit(
                            f"{name} exited unexpectedly (rc={proc.returncode}); "
                            f"see {logs_dir}"
                        )
                time.sleep(0.5)
            apply_phase_exit(phase, control_url, consumer_count, db_url)

        tracker.set("final_drain", "recovery")
        log("stopping loadgen; waiting for ledger dump…")
        terminate(loadgen, "loadgen")
        if not ledger_path.exists():
            raise SystemExit("loadgen did not write its ledger")

        log(f"draining + verifying ({args.drain_timeout_s:.0f}s budget)…")
        verdict = drain_and_verify(
            receiver_base, consumer_count, ledger_path, args.drain_timeout_s
        )
        exit_code = 0 if verdict["pass"] else 2
    finally:
        terminate(loadgen, "loadgen")
        # Stop in reverse launch order: a consumer process must go down
        # before the state store it depends on. Sequin appends [redis,
        # sequin], so stopping forward kills Redis first and Sequin then
        # floods its log with cursor-commit failures during its own
        # shutdown (harmless but misleading in forensics).
        for managed in reversed(adapter_procs):
            managed.stop()
        terminate(receiver, "receiver")
        if poller is not None:
            poller.stop_event.set()
            poller.join(timeout=5)
        if resources is not None:
            resources.stop_event.set()
            resources.join(timeout=5)
        # Defensively restore the cluster WAL-retention cap. The
        # slot-invalidation phase lowers max_slot_wal_keep_size via ALTER
        # SYSTEM and only resets it on normal phase exit; if the run aborts
        # mid-phase (adapter death, verify abort, Ctrl-C) the low cap would
        # leak into later cells and fake slot loss. RESET is a no-op when the
        # cap was never touched.
        if any(p.type is PhaseType.SLOT_INVALIDATION for p in phases):
            try:
                with psycopg.connect(db_url, autocommit=True) as conn:
                    with conn.cursor() as cur:
                        cur.execute("ALTER SYSTEM RESET max_slot_wal_keep_size")
                        cur.execute("SELECT pg_reload_conf()")
            except Exception as exc:
                log(f"cleanup: could not reset max_slot_wal_keep_size: {exc}")
        sink.flush_close()

        summary = compute_summary(
            out_dir / "raw.csv", run_id=run_id,
            scenario=args.scenario, phases=phases,
        )
        summary["cdc_verify"] = verdict
        write_summary(summary, out_dir / "summary.json")

        # SUT-emitted fields win; harness knowledge fills the gaps.
        descriptor: dict = {}
        if len(instance_descriptors) == 1:
            descriptor.update(next(iter(instance_descriptors.values())))
        elif instance_descriptors:
            descriptor["instances"] = instance_descriptors
        descriptor.setdefault("system", entry.system)
        descriptor.setdefault("slot_names", expected_slots)
        descriptor.setdefault("version", entry.version(entry))
        descriptor.setdefault("topology", entry.topology)
        descriptor["snapshot_mode_requested"] = args.snapshot_mode
        descriptor["snapshot_mode_effective"] = effective_snapshot
        manifest = build_manifest(
            run_id=run_id, scenario=args.scenario, phases=phases,
            systems=[args.system], database_url=db_url,
            cli_args=argv if argv is not None else sys.argv[1:],
            adapter_versions={args.system: {"descriptor": descriptor}},
            pg_image="(see docker-compose.yml)",
        )
        manifest["cdc"] = {
            "consumer_profiles": args.profiles,
            "rate": args.rate,
            "op_mix": args.op_mix,
            "key_cardinality": args.key_cardinality,
            "payload_bytes": args.payload_bytes,
            "mode": args.mode,
            "snapshot_mode_requested": args.snapshot_mode,
            "snapshot_mode_effective": effective_snapshot,
        }
        write_manifest(manifest, out_dir / "manifest.json")
        (out_dir / "README.md").write_text(
            f"# CDC bench run `{run_id}`\n\n"
            f"- System: `{args.system}` — scenario `{args.scenario or 'custom'}`\n"
            f"- Consumers: {consumer_count} ({args.profiles}) at {args.rate} ops/s "
            f"({args.op_mix} insert/update/delete)\n"
            f"- Verify: **{'PASS' if verdict.get('pass') else 'FAIL'}** "
            f"(details in `summary.json -> cdc_verify`)\n\n"
            f"Files: `raw.csv`, `summary.json`, `manifest.json`, `ledger.json`, `logs/`.\n"
            f"Rerun: `uv run cdc {shlex.join(argv if argv is not None else sys.argv[1:])}`\n"
        )
        log(f"results in {out_dir}")

    for cid, res in verdict.get("consumers", {}).items():
        tx_note = (f" torn_txs={res['torn_txs_open']}"
                   if res.get("txs_completed") else "")
        log(f"consumer {cid} [{res['profile']}]: delivered={res['delivered']} "
            f"dups={res['dups']} order_violations={res['order_violations']} "
            f"lost_events={res['lost_events']} "
            f"balance_mismatches={res['balance_mismatches']}{tx_note} -> "
            f"{'PASS' if res['pass'] else 'FAIL'}")
    log(f"verification: {'PASS' if verdict.get('pass') else 'FAIL'}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
