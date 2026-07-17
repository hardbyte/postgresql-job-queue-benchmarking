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


def preflight(admin_url: str, db_name: str, slot_prefix: str,
              extra_databases: tuple[str, ...] = (),
              precreate_slots: tuple[str, ...] = ()) -> str:
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
            cur.execute("""
                CREATE TABLE cdc_bench.events (
                    pk         bigint PRIMARY KEY,
                    seq        bigint NOT NULL,
                    tx_id      bigint NOT NULL,
                    payload    bytea  NOT NULL,
                    emitted_us bigint NOT NULL
                )""")
            cur.execute("DROP PUBLICATION IF EXISTS cdc_pub")
            cur.execute("CREATE PUBLICATION cdc_pub FOR TABLE cdc_bench.events")
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


# ── Phase hooks (consumer chaos via receiver control API) ────────────────


def apply_phase_enter(phase: Phase, control_url: str, consumer_count: int) -> None:
    if phase.type is PhaseType.CONSUMER_DEAD:
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


def apply_phase_exit(phase: Phase, control_url: str, consumer_count: int) -> None:
    if phase.type in (PhaseType.CONSUMER_DEAD, PhaseType.CONSUMER_SLOW):
        cid = phase.int_param("id", 0)
        http_json(control_url, {"consumer_id": cid, "mode": "ok"})
        log(f"phase {phase.label}: consumer {cid} -> ok")
    elif phase.type is PhaseType.SINK_OUTAGE:
        for cid in range(consumer_count):
            http_json(control_url, {"consumer_id": cid, "mode": "ok"})
        log(f"phase {phase.label}: all consumers -> ok")


# ── Drain verification ───────────────────────────────────────────────────


def verify_consumer(source: dict, receiver: dict) -> dict:
    """Compare the loadgen ledger against one consumer's delivered state."""
    last_seq = receiver.get("last_seq", {})
    deleted = set(receiver.get("deleted", []))
    lost_events = 0
    lost_keys = 0
    missed_deletes = 0
    for pk_str, entry in source["keys"].items():
        pk = int(pk_str)
        if entry["deleted"]:
            if pk not in deleted:
                missed_deletes += 1
            continue
        got = last_seq.get(pk_str)
        if got is None:
            lost_keys += 1
            lost_events += entry["seq"]
        elif got < entry["seq"]:
            lost_events += entry["seq"] - got
            lost_keys += 1
    return {
        "profile": receiver.get("profile"),
        "delivered": receiver.get("delivered"),
        "dups": receiver.get("dups"),
        "order_violations": receiver.get("order_violations"),
        "lost_keys": lost_keys,
        "lost_events": lost_events,
        "missed_deletes": missed_deletes,
        "pass": lost_keys == 0 and lost_events == 0 and missed_deletes == 0,
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
    receiver_bin = receiver_binary()
    entry.prepare(entry)
    ensure_postgres(compose=not args.skip_pg_setup)
    db_url = preflight(args.admin_url, entry.db_name, entry.slot_prefix,
                       entry.extra_databases, entry.precreate_slots)
    admin_parts = urllib.parse.urlsplit(args.admin_url)

    tracker = PhaseTracker()
    sink = SampleSink(out_dir / "raw.csv", run_id, args.system, tracker)
    receiver_base = f"http://127.0.0.1:{args.port}"
    control_url = f"{receiver_base}/control"
    ledger_path = out_dir / "ledger.json"

    receiver = loadgen = None
    adapter_procs: list[ManagedProc] = []
    poller = None
    descriptor: dict = {}
    verdict: dict = {"pass": False, "error": "run did not reach verification"}
    exit_code = 1
    try:
        receiver = subprocess.Popen(
            [str(receiver_bin)],
            env={**os.environ,
                 "CDC_PORT": str(args.port),
                 "CONSUMER_PROFILES": args.profiles,
                 "ENVELOPE": entry.envelope,
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

        ctx = LaunchCtx(
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
                tail_jsonl(managed.proc, sink, managed.name,
                           on_descriptor=descriptor.update)

        expected_slots = entry.slot_names(consumer_count)
        wait_for_slots(db_url, expected_slots, adapter_procs,
                       timeout_s=args.adapter_ready_timeout_s, logs_dir=logs_dir,
                       min_count=consumer_count)
        log(f"adapter up: slots={expected_slots or f'{consumer_count} (names SUT-derived)'}")
        descriptor.setdefault("system", entry.system)
        descriptor.setdefault("slot_names", expected_slots)
        descriptor.setdefault("version", entry.version(entry))
        descriptor.setdefault("topology", entry.topology)

        loadgen = subprocess.Popen(
            [sys.executable, "-m", "cdc_harness.loadgen",
             "--database-url", db_url,
             "--rate", str(args.rate),
             "--op-mix", args.op_mix,
             "--key-cardinality", str(args.key_cardinality),
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

        for phase in phases:
            tracker.set(phase.label, phase.type.value)
            log(f"phase {phase.label} ({phase.type.value}) for {phase.duration_s}s")
            apply_phase_enter(phase, control_url, consumer_count)
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
            apply_phase_exit(phase, control_url, consumer_count)

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
        for managed in adapter_procs:
            managed.stop()
        terminate(receiver, "receiver")
        if poller is not None:
            poller.stop_event.set()
            poller.join(timeout=5)
        sink.flush_close()

        summary = compute_summary(
            out_dir / "raw.csv", run_id=run_id,
            scenario=args.scenario, phases=phases,
        )
        summary["cdc_verify"] = verdict
        write_summary(summary, out_dir / "summary.json")
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
        log(f"consumer {cid} [{res['profile']}]: delivered={res['delivered']} "
            f"dups={res['dups']} order_violations={res['order_violations']} "
            f"lost_events={res['lost_events']} -> "
            f"{'PASS' if res['pass'] else 'FAIL'}")
    log(f"verification: {'PASS' if verdict.get('pass') else 'FAIL'}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
