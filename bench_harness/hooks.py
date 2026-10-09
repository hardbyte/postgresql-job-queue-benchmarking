"""Phase-type enter/exit runtime hooks.

Each hook receives a PhaseRuntime and may stash state in runtime.state that
its paired exit hook retrieves to clean up.

Hooks run synchronously on the orchestrator thread — they should complete
fast. Long-running side effects (held transactions, background readers) get
forked into threads that block on an event the exit hook signals.
"""

from __future__ import annotations

import math
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

import psycopg

from .adapters import DEFAULT_PG_IMAGE, PG_PASS, PG_PORT, PG_USER
from .metrics import NO_REPLICA_IDENTITY_PREDICATE, PUBLISHED_TABLES_FROM
from .phases import PhaseRuntime


# ─── idle-in-tx ──────────────────────────────────────────────────────────
#
# Open a connection, BEGIN, SELECT txid_current(), sleep until the exit hook
# closes the connection. The transaction pins the cluster xmin horizon for
# the whole phase.


def enter_idle_in_tx(runtime: PhaseRuntime) -> None:
    stop = threading.Event()
    ready = threading.Event()
    holder: dict[str, Any] = {"stop": stop, "ready": ready}

    def _hold() -> None:
        try:
            # autocommit off: the transaction stays open until close().
            with psycopg.connect(runtime.database_url, autocommit=False) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT txid_current()")
                    xid = cur.fetchone()[0]
                    holder["xid"] = xid
                # Signal success only after xid is captured: if BEGIN/SELECT
                # raised, the ready event is still set below with an error
                # so the caller can surface it instead of silently running a
                # no-op idle-in-tx phase.
                ready.set()
                # Block until the exit hook signals us. Don't commit/rollback
                # until then: autoexit via `with` rollback fires on stop.
                stop.wait()
        except Exception as exc:
            holder["error"] = exc
            ready.set()

    thread = threading.Thread(target=_hold, name="idle-in-tx-holder", daemon=True)
    thread.start()
    holder["thread"] = thread
    runtime.state["idle-in-tx"] = holder

    def _abort_holder() -> None:
        # The registry's exit hook won't run if we raise, so tear the
        # holder down here. Otherwise the thread could finish its connect
        # later and pin a transaction across subsequent phases.
        stop.set()
        runtime.state.pop("idle-in-tx", None)
        thread.join(timeout=1.0)

    # Wait until the holder either opens its transaction or errors out. The
    # phase is measuring "what happens when the MVCC horizon is pinned," so
    # silently running without a held transaction would make the measurement
    # meaningless.
    if not ready.wait(timeout=5.0):
        _abort_holder()
        raise RuntimeError(
            "idle-in-tx holder thread did not open a transaction within 5s"
        )
    if "error" in holder:
        err = holder["error"]
        _abort_holder()
        raise RuntimeError(
            f"idle-in-tx holder thread failed to open transaction: {err}"
        ) from err


def exit_idle_in_tx(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("idle-in-tx", None)
    if not holder:
        return
    holder["stop"].set()
    thread: threading.Thread = holder["thread"]
    thread.join(timeout=5.0)


# ─── active-readers ──────────────────────────────────────────────────────
#
# Open N overlapping REPEATABLE READ connections running a repeating scan
# query. Parity with awa's Rust MVCC bench `active_scan` mode.


def _reader_loop(
    database_url: str,
    stop: threading.Event,
    scan_sql: str,
) -> None:
    with psycopg.connect(database_url, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            while not stop.is_set():
                try:
                    cur.execute(scan_sql)
                    cur.fetchall()
                except psycopg.Error:
                    # Swallow transient errors; keep the reader active.
                    pass
                time.sleep(0.1)


def enter_active_readers(runtime: PhaseRuntime) -> None:
    # The PlanetScale failure mode is about analytics readers hitting the
    # queue's hot tables while the primary churns them. Default to scanning
    # the first event table from the adapter's manifest so the reader
    # actually exercises that path; fall back to a catalog scan only if
    # the manifest didn't declare any event tables. Callers can still
    # override via env for bespoke analytics shapes.
    event_tables = runtime.state.get("event_tables") or []
    default_sql: str
    if event_tables:
        first = str(event_tables[0])
        default_sql = f"SELECT count(*) FROM {first}"
    else:
        default_sql = "SELECT count(*) FROM pg_stat_user_tables"
    scan_sql = os.environ.get("ACTIVE_READER_SQL", default_sql)
    reader_count = int(os.environ.get("ACTIVE_READER_COUNT", "4"))
    stop = threading.Event()
    threads: list[threading.Thread] = []
    for i in range(reader_count):
        t = threading.Thread(
            target=_reader_loop,
            args=(runtime.database_url, stop, scan_sql),
            name=f"active-reader-{i}",
            daemon=True,
        )
        t.start()
        threads.append(t)
    runtime.state["active-readers"] = {"stop": stop, "threads": threads}


def exit_active_readers(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("active-readers", None)
    if not holder:
        return
    holder["stop"].set()
    for t in holder["threads"]:
        t.join(timeout=5.0)


# ─── high-load ───────────────────────────────────────────────────────────
#
# Signal the adapter to raise its producer rate for the duration of the
# phase. We write `PRODUCER_TARGET_RATE=<new>` to a well-known path that
# the adapter re-reads on each producer tick. Adapters that don't support
# dynamic rate changes simply ignore the file — the phase still runs the
# clean workload, which is strictly worse data but not a failure.


def enter_high_load(runtime: PhaseRuntime) -> None:
    multiplier = float(runtime.state.get("high_load_multiplier", 1.5))
    base = float(runtime.state.get("base_producer_rate", 800.0))
    control_file = runtime.state.get("producer_rate_control_file")
    if not control_file:
        return
    with Path(control_file).open("w") as fh:
        fh.write(str(base * multiplier))


def exit_high_load(runtime: PhaseRuntime) -> None:
    base = str(runtime.state.get("base_producer_rate", 800))
    control_file = runtime.state.get("producer_rate_control_file")
    if not control_file:
        return
    with Path(control_file).open("w") as fh:
        fh.write(base)


# ─── kill-worker / start-worker ──────────────────────────────────────────
#
# Destructive / lifecycle phase types. Both act on the replica pool stashed
# on `runtime.state["replica_pool"]` by `orchestrator.run_one_system`. The
# `instance` param (default 0) selects which replica; phase parsing lets
# a scenario write `kill=kill-worker(instance=2):60s`.
#
# Neither has an exit hook — the enter-side action is the whole story.
# Restart-on-exit would conflate "kill" with "kill then restart" and make
# the named-scenario composition (crash_recovery, rolling-replace) harder
# to reason about. Scenarios that need restart follow a kill-worker phase
# with a start-worker phase.
#
# A ValueError from the pool (out-of-range instance_id, already running for
# start-worker, etc.) propagates up through the phase loop as a hard abort;
# that's appropriate — a scenario targeting a non-existent replica is
# misconfigured, not a chaos data point.


def enter_kill_worker(runtime: PhaseRuntime) -> None:
    pool = runtime.state.get("replica_pool")
    if pool is None:
        # Importing the type here would create a circular phases→replica_pool
        # dependency; duck-type instead.
        raise RuntimeError(
            "kill-worker requires state['replica_pool']. "
            "The orchestrator sets this in run_one_system; running a "
            "kill-worker phase outside that context is a misconfiguration."
        )
    instance = runtime.phase.int_param("instance", default=0)
    pool.kill_worker(instance)


def enter_start_worker(runtime: PhaseRuntime) -> None:
    pool = runtime.state.get("replica_pool")
    if pool is None:
        raise RuntimeError(
            "start-worker requires state['replica_pool']. "
            "The orchestrator sets this in run_one_system; running a "
            "start-worker phase outside that context is a misconfiguration."
        )
    instance = runtime.phase.int_param("instance", default=0)
    pool.start_worker(instance)


# ─── postgres-restart ────────────────────────────────────────────────────
#
# Take Postgres down for the first half of the phase duration, then bring
# it back up for the remainder. Drives the compose `postgres_restart_fn`
# the orchestrator stashed on runtime.state so the hook stays free of
# compose / docker knowledge.


def enter_postgres_restart(runtime: PhaseRuntime) -> None:
    restart_fn = runtime.state.get("postgres_restart_fn")
    if restart_fn is None:
        raise RuntimeError(
            "postgres-restart requires state['postgres_restart_fn']. "
            "The orchestrator sets this in run_one_system; running a "
            "postgres-restart phase outside that context is a misconfiguration."
        )
    duration_s = runtime.phase.duration_s
    # Stop immediately on enter; the harness phase loop sleeps for the
    # full duration after the enter hook returns. We schedule the
    # restart on a background thread so half the phase is "down" and
    # the second half is "back up + measuring recovery in-phase."
    stop_event = threading.Event()
    holder: dict[str, Any] = {"stop": stop_event, "error": None}

    # Identify which compose helper to use — the orchestrator provides
    # the compound restart_fn (stop + start). For the first-half-down
    # behaviour we need the two halves separately. Pull the underlying
    # callables off state if present; otherwise fall back to invoking
    # restart_fn (which is a stop+start) at t=0 and accepting the
    # phase being mostly "down then up at the very end" — this fallback
    # path is only taken in tests that don't wire the helpers in.
    stop_fn = runtime.state.get("postgres_stop_fn")
    start_fn = runtime.state.get("postgres_start_fn")

    def _run() -> None:
        try:
            half = max(1.0, duration_s / 2.0)
            if stop_fn:
                stop_fn()
            else:
                # No split helpers: do the whole stop+start now and
                # treat the rest of the phase as recovery observation.
                restart_fn()
                return
            # Wait for the first-half mark or an early-exit signal.
            if stop_event.wait(timeout=half):
                # Phase ended early; bring PG back up regardless.
                pass
            if start_fn:
                start_fn()
        except Exception as exc:  # surfaced by the exit hook
            holder["error"] = exc

    thread = threading.Thread(
        target=_run, name="postgres-restart-driver", daemon=True
    )
    thread.start()
    holder["thread"] = thread
    runtime.state["postgres-restart"] = holder


def exit_postgres_restart(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("postgres-restart", None)
    if not holder:
        return
    holder["stop"].set()
    thread: threading.Thread = holder["thread"]
    # Generous join — start_postgres in the orchestrator can take ~30s
    # on a cold image pull. We must not return from exit before PG is
    # back up; the next phase's adapter samples would fail otherwise.
    thread.join(timeout=120.0)
    if holder.get("error"):
        raise RuntimeError(
            f"postgres-restart driver thread failed: {holder['error']}"
        ) from holder["error"]


# ─── pg-backend-kill ─────────────────────────────────────────────────────
#
# Open one sampler connection (admin DB) that runs `pg_terminate_backend`
# against the system-under-test's backends every `1/rate` seconds.
# Targets backends connected to the system's database (datname filter)
# rather than relying on application_name, which adapters don't
# uniformly set.


def _pg_backend_kill_loop(
    admin_url: str,
    target_db: str,
    rate: float,
    stop: threading.Event,
    holder: dict[str, Any],
) -> None:
    period = 1.0 / max(rate, 0.01)
    sql = (
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname = %s "
        "AND pid <> pg_backend_pid() "
        "AND state IN ('active', 'idle in transaction')"
    )
    try:
        with psycopg.connect(admin_url, autocommit=True) as conn:
            with conn.cursor() as cur:
                while not stop.is_set():
                    try:
                        cur.execute(sql, (target_db,))
                        cur.fetchall()
                        holder["kills"] = holder.get("kills", 0) + (cur.rowcount or 0)
                    except psycopg.Error:
                        # PG might bounce mid-loop in combined chaos
                        # scenarios; reconnect on the next tick.
                        break
                    if stop.wait(timeout=period):
                        return
    except psycopg.Error as exc:
        holder["error"] = exc


def enter_pg_backend_kill(runtime: PhaseRuntime) -> None:
    admin_url = runtime.state.get("admin_database_url") or runtime.database_url
    target_db = runtime.state.get("system_database_name")
    if not target_db:
        raise RuntimeError(
            "pg-backend-kill requires state['system_database_name']."
        )
    rate = float(runtime.phase.param("rate", "2"))
    stop = threading.Event()
    holder: dict[str, Any] = {"stop": stop}
    thread = threading.Thread(
        target=_pg_backend_kill_loop,
        args=(admin_url, str(target_db), rate, stop, holder),
        name="pg-backend-kill",
        daemon=True,
    )
    thread.start()
    holder["thread"] = thread
    runtime.state["pg-backend-kill"] = holder


def exit_pg_backend_kill(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("pg-backend-kill", None)
    if not holder:
        return
    holder["stop"].set()
    holder["thread"].join(timeout=5.0)


# ─── pool-exhaustion ─────────────────────────────────────────────────────
#
# Open N idle connections held against the system-under-test's database
# for the phase duration. Verifies that the SUT survives connection
# pressure (and that its own pool sizing leaves headroom).
#
# Connections are opened best-effort: if PG's `max_connections` is below
# the requested count we open as many as we can and log the shortfall
# rather than aborting — the chaos point is "what happens under
# pressure," not "fail the run if PG can't fit our request."


def _pool_exhaustion_holder(
    database_url: str,
    n: int,
    ready: threading.Event,
    stop: threading.Event,
    holder: dict[str, Any],
) -> None:
    conns: list[psycopg.Connection] = []
    try:
        for _ in range(n):
            try:
                conn = psycopg.connect(database_url, autocommit=True)
                conns.append(conn)
            except psycopg.Error as exc:
                holder.setdefault("errors", []).append(str(exc))
                break
        holder["opened"] = len(conns)
        ready.set()
        stop.wait()
    finally:
        for c in conns:
            try:
                c.close()
            except Exception:
                pass


def enter_pool_exhaustion(runtime: PhaseRuntime) -> None:
    n = runtime.phase.int_param("idle_conns", default=300)
    db_url = runtime.state.get("system_database_url") or runtime.database_url
    stop = threading.Event()
    ready = threading.Event()
    holder: dict[str, Any] = {"stop": stop, "ready": ready}
    thread = threading.Thread(
        target=_pool_exhaustion_holder,
        args=(db_url, n, ready, stop, holder),
        name="pool-exhaustion",
        daemon=True,
    )
    thread.start()
    holder["thread"] = thread
    runtime.state["pool-exhaustion"] = holder
    # Don't block the phase loop on full pool fill; the holder thread
    # races to open connections in the background. A short ready-wait
    # ensures we've at least started before the phase clock advances.
    ready.wait(timeout=10.0)


def exit_pool_exhaustion(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("pool-exhaustion", None)
    if not holder:
        return
    holder["stop"].set()
    holder["thread"].join(timeout=10.0)


# ─── repeated-kill ───────────────────────────────────────────────────────
#
# Periodic SIGKILL + auto-restart of replica I every `period` seconds
# throughout phase duration. Composes the existing replica pool kill /
# start operations so it stays consistent with `crash_recovery`.


def _repeated_kill_loop(
    pool: Any,
    instance: int,
    period_s: float,
    stop: threading.Event,
    holder: dict[str, Any],
) -> None:
    try:
        while not stop.is_set():
            if stop.wait(timeout=period_s):
                return
            try:
                pool.kill_worker(instance)
            except Exception as exc:
                holder.setdefault("errors", []).append(f"kill: {exc}")
                continue
            # Brief pause to let the kill register before restarting.
            if stop.wait(timeout=1.0):
                return
            try:
                pool.start_worker(instance)
                holder["cycles"] = holder.get("cycles", 0) + 1
            except Exception as exc:
                holder.setdefault("errors", []).append(f"start: {exc}")
    except Exception as exc:
        holder["error"] = exc


def enter_repeated_kill(runtime: PhaseRuntime) -> None:
    pool = runtime.state.get("replica_pool")
    if pool is None:
        raise RuntimeError(
            "repeated-kill requires state['replica_pool']."
        )
    instance = runtime.phase.int_param("instance", default=0)
    period_raw = runtime.phase.param("period", "20s")
    # Reuse the same duration parser the DSL uses so `period=20s` /
    # `period=1m` / `period=45` all do the right thing.
    from .phases import parse_duration

    period_s = float(parse_duration(period_raw))
    stop = threading.Event()
    holder: dict[str, Any] = {"stop": stop, "instance": instance}
    thread = threading.Thread(
        target=_repeated_kill_loop,
        args=(pool, instance, period_s, stop, holder),
        name="repeated-kill",
        daemon=True,
    )
    thread.start()
    holder["thread"] = thread
    runtime.state["repeated-kill"] = holder


def exit_repeated_kill(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("repeated-kill", None)
    if not holder:
        return
    holder["stop"].set()
    holder["thread"].join(timeout=10.0)
    # Ensure the targeted replica is RUNNING when we leave the phase —
    # otherwise the recovery phase would show artificial throughput
    # depression caused by a still-dead replica, not by the chaos.
    pool = runtime.state.get("replica_pool")
    if pool is None:
        return
    # Best-effort: pool.start_worker raises if it's already running.
    instance = holder.get("instance")
    if instance is None:
        return
    try:
        pool.start_worker(instance)
    except Exception:
        pass


# ─── shared helpers for harness-side Postgres clients ────────────────────
#
# pgbench and pg_recvlogical run as sidecar containers from the pinned stock
# Postgres image (whatever --engine is under test) on the host network, so
# no host install is needed and the client's CPU isn't charged to the
# Postgres container's cgroup. Container names carry the harness port so
# parallel harnesses (BENCH_PG_PORT) don't collide.

CLIENT_IMAGE = DEFAULT_PG_IMAGE


def _client_container_name(role: str) -> str:
    return f"bench-{role}-{PG_PORT}"


def _client_conn_args() -> list[str]:
    return ["-h", "127.0.0.1", "-p", str(PG_PORT), "-U", PG_USER]


def _docker_client_argv(
    name: str, command: list[str], *, docker_args: Iterable[str] = ()
) -> list[str]:
    return [
        "docker", "run", "--rm", "--name", name, "--network", "host",
        "-e", f"PGPASSWORD={PG_PASS}", *docker_args, CLIENT_IMAGE, *command,
    ]


def _docker_rm_force(name: str) -> None:
    subprocess.run(
        ["docker", "rm", "-f", name], capture_output=True, check=False
    )


def _set_producer_rate(state: dict[str, object], rate: float) -> None:
    control_file = state.get("producer_rate_control_file")
    if control_file:
        Path(str(control_file)).write_text(str(rate))


def _apply_load_param(runtime: PhaseRuntime) -> None:
    """`load=X` sets the producer rate to X × --producer-rate for the phase."""
    load = runtime.phase.float_param("load", 1.0)
    if load < 0:
        raise ValueError(f"phase {runtime.phase.label!r}: load must be >= 0")
    base = float(runtime.state.get("base_producer_rate", 800.0))
    _set_producer_rate(runtime.state, base * load)


def _restore_base_rate(runtime: PhaseRuntime) -> None:
    _set_producer_rate(
        runtime.state, float(runtime.state.get("base_producer_rate", 800.0))
    )


def _emit(state: dict[str, object]) -> Callable[..., None]:
    emit = state.get("emit_sample")
    if emit is None:
        raise RuntimeError(
            "state['emit_sample'] is missing; the orchestrator sets it in "
            "run_one_system."
        )
    return emit  # type: ignore[return-value]


# ─── neighbour-oltp ──────────────────────────────────────────────────────
#
# A rate-limited pgbench workload against a separate `neighbour` database on
# the same server, one pgbench run per phase. With --rate the per-transaction
# latency pgbench logs is measured from the scheduled start, so it includes
# any queueing the neighbour suffers (no coordinated omission). Latency, not
# throughput, is the signal: TPS only drops if the server can't keep up.

NEIGHBOUR_DB = "neighbour"


@dataclass(frozen=True)
class PgbenchTxn:
    end_epoch_s: float
    latency_ms: float | None  # None: failed / skipped transaction


def parse_pgbench_log_line(line: str) -> PgbenchTxn | None:
    """Parse one pgbench per-transaction log line:
    `client_id txn_no time script_no epoch_s epoch_us [schedule_lag]`.
    `time` is the latency in µs, or `failed` / `skipped` / error class."""
    parts = line.split()
    if len(parts) < 6:
        return None
    try:
        end_epoch_s = int(parts[4]) + int(parts[5]) / 1e6
    except ValueError:
        return None
    try:
        latency_ms: float | None = int(parts[2]) / 1000.0
    except ValueError:
        latency_ms = None
    return PgbenchTxn(end_epoch_s=end_epoch_s, latency_ms=latency_ms)


def _percentile(sorted_values: list[float], q: float) -> float | None:
    """Nearest-rank percentile of an ascending list."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(q / 100.0 * len(sorted_values)))
    return sorted_values[min(rank, len(sorted_values)) - 1]


def neighbour_phase_stats(txns: list[PgbenchTxn], duration_s: float) -> dict[str, float]:
    latencies = sorted(t.latency_ms for t in txns if t.latency_ms is not None)
    failed = sum(1 for t in txns if t.latency_ms is None)
    stats: dict[str, float] = {
        "neighbour_phase_transactions": float(len(latencies)),
        "neighbour_phase_failed": float(failed),
        "neighbour_phase_tps": len(latencies) / duration_s if duration_s > 0 else 0.0,
    }
    if latencies:
        stats["neighbour_phase_latency_mean_ms"] = sum(latencies) / len(latencies)
        stats["neighbour_phase_latency_max_ms"] = latencies[-1]
        for q in (50, 95, 99):
            stats[f"neighbour_phase_latency_p{q}_ms"] = _percentile(latencies, q)  # type: ignore[assignment]
    return stats


def neighbour_window_stats(
    txns: list[PgbenchTxn], window_s: float
) -> list[tuple[float, dict[str, float]]]:
    """Per-window (end_epoch, {tps, p50, p99}) on wall-clock-aligned
    windows. The first and last windows are dropped when partial, so the
    series' TPS isn't depressed by pgbench start-up / shutdown."""
    if not txns or window_s <= 0:
        return []
    first = min(t.end_epoch_s for t in txns)
    last = max(t.end_epoch_s for t in txns)
    buckets: dict[int, list[PgbenchTxn]] = {}
    for txn in txns:
        buckets.setdefault(int(txn.end_epoch_s // window_s), []).append(txn)
    out: list[tuple[float, dict[str, float]]] = []
    for index in sorted(buckets):
        start, end = index * window_s, (index + 1) * window_s
        if start < first or end > last:
            continue
        latencies = sorted(
            t.latency_ms for t in buckets[index] if t.latency_ms is not None
        )
        metrics = {"neighbour_tps": len(latencies) / window_s}
        if latencies:
            metrics["neighbour_latency_p50_ms"] = _percentile(latencies, 50)  # type: ignore[assignment]
            metrics["neighbour_latency_p99_ms"] = _percentile(latencies, 99)  # type: ignore[assignment]
        out.append((end, metrics))
    return out


def prepare_neighbour_oltp(state: dict[str, object]) -> None:
    """(Re)create the neighbour database and run `pgbench -i` once per
    system, so its WAL lands in warmup rather than a measured phase."""
    admin_url = str(state.get("admin_database_url") or "")
    scale = int(os.environ.get("NEIGHBOUR_PGBENCH_SCALE", "10"))
    with psycopg.connect(admin_url, autocommit=True) as conn:
        conn.execute(f'DROP DATABASE IF EXISTS "{NEIGHBOUR_DB}" WITH (FORCE)')
        conn.execute(f'CREATE DATABASE "{NEIGHBOUR_DB}"')
    name = _client_container_name("pgbench")
    _docker_rm_force(name)
    print(
        f"[harness] initialising {NEIGHBOUR_DB} with pgbench -i -s {scale}",
        file=sys.stderr,
    )
    proc = subprocess.run(
        _docker_client_argv(
            name,
            ["pgbench", "-i", "-q", "-s", str(scale), *_client_conn_args(), NEIGHBOUR_DB],
        ),
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"pgbench -i failed (rc={proc.returncode}): {proc.stderr[-2000:]}"
        )


def _pgbench_script_args(script: str) -> list[str]:
    """`tpcb-like` or `select-only@9+simple-update@1` → repeated -b flags.
    `+` separates scripts because `,` separates phase params."""
    args: list[str] = []
    for part in script.split("+"):
        part = part.strip()
        if part:
            args += ["-b", part]
    return args


def enter_neighbour_oltp(runtime: PhaseRuntime) -> None:
    phase = runtime.phase
    clients = phase.int_param("clients", 8)
    rate = phase.int_param("rate", 300)
    script = phase.param("script", "tpcb-like") or "tpcb-like"
    threads = max(1, min(clients, 2))
    _apply_load_param(runtime)
    log_dir = Path(tempfile.mkdtemp(prefix="bench-neighbour-"))
    name = _client_container_name("pgbench")
    _docker_rm_force(name)
    argv = _docker_client_argv(
        name,
        [
            "pgbench", *_client_conn_args(), "-n",
            "-c", str(clients), "-j", str(threads),
            "-T", str(phase.duration_s), "-R", str(rate),
            "-l", "--log-prefix=/out/pgbench",
            *_pgbench_script_args(script),
            NEIGHBOUR_DB,
        ],
        docker_args=[
            "--user", f"{os.getuid()}:{os.getgid()}",
            "-v", f"{log_dir}:/out",
        ],
    )
    output = (log_dir / "pgbench.out").open("w")
    proc = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT)
    runtime.state["neighbour-oltp"] = {
        "proc": proc,
        "output": output,
        "log_dir": log_dir,
        "name": name,
    }


def exit_neighbour_oltp(runtime: PhaseRuntime) -> None:
    holder = runtime.state.pop("neighbour-oltp", None)
    _restore_base_rate(runtime)
    if not holder:
        return
    proc: subprocess.Popen = holder["proc"]
    log_dir: Path = holder["log_dir"]
    try:
        # pgbench was started with -T = phase duration, so it normally exits
        # within a second or two of the phase loop's sleep.
        try:
            proc.wait(timeout=30.0)
        except subprocess.TimeoutExpired:
            _docker_rm_force(holder["name"])
            proc.wait(timeout=10.0)
        holder["output"].close()
        output = (log_dir / "pgbench.out").read_text(errors="replace")
        if proc.returncode != 0:
            print(
                f"[harness] neighbour pgbench exited rc={proc.returncode}:\n"
                f"{output[-2000:]}",
                file=sys.stderr,
            )
        txns: list[PgbenchTxn] = []
        for log_file in sorted(log_dir.glob("pgbench.*")):
            if log_file.name == "pgbench.out":
                continue
            with log_file.open() as fh:
                for line in fh:
                    txn = parse_pgbench_log_line(line)
                    if txn is not None:
                        txns.append(txn)
        emit = _emit(runtime.state)
        window_s = float(runtime.state.get("sample_every_s", 5))
        for end_epoch, metrics in neighbour_window_stats(txns, window_s):
            for metric, value in metrics.items():
                emit("neighbour", "", metric, value, window_s=window_s, at_epoch=end_epoch)
        stats = neighbour_phase_stats(txns, float(runtime.phase.duration_s))
        stats["neighbour_phase_exit_code"] = float(proc.returncode or 0)
        for metric, value in stats.items():
            emit("neighbour", "", metric, value, window_s=float(runtime.phase.duration_s))
        print(
            f"[harness] neighbour {runtime.phase.label}: "
            + ", ".join(f"{k.removeprefix('neighbour_phase_')}={v:.2f}" for k, v in stats.items()),
            file=sys.stderr,
        )
    finally:
        shutil.rmtree(log_dir, ignore_errors=True)


# ─── logical-stream / logical-stall ──────────────────────────────────────
#
# A FOR ALL TABLES publication plus a pgoutput slot on the system's
# database, consumed by pg_recvlogical (output discarded, bytes counted).
# Created by the first logical phase and kept until the system's run ends:
# slot lag, retained WAL and catalog_xmin need continuity across phases.
# logical-stall freezes the consumer container (`docker pause`) for its own
# phase: the walsender stays connected until wal_sender_timeout, the slot
# stops advancing, and the consumer reconnects on resume. Slot and catalog
# state are sampled by the metrics daemon; consumer-side counters are
# emitted here.

LOGICAL_SLOT = "bench_cdc_slot"
LOGICAL_PUBLICATION = "bench_cdc_pub"


class LogicalConsumer:
    def __init__(
        self,
        *,
        database_name: str,
        emit: Callable[..., None],
        sample_every_s: float,
    ) -> None:
        self.database_name = database_name
        self.emit = emit
        self.sample_every_s = sample_every_s
        self.name = _client_container_name("recvlogical")
        self.proc: subprocess.Popen | None = None
        self.paused = False
        self.bytes_total = 0
        self.disconnects_total = 0
        self.errors_total = 0
        self.exits_total = 0
        self.last_error = ""
        self._stopping = threading.Event()
        self._monitor = threading.Thread(
            target=self._monitor_loop, name="logical-consumer-monitor", daemon=True
        )

    def _argv(self) -> list[str]:
        return _docker_client_argv(
            self.name,
            [
                "pg_recvlogical", *_client_conn_args(),
                "-d", self.database_name, "--slot", LOGICAL_SLOT, "--start",
                "-o", "proto_version=1",
                "-o", f"publication_names={LOGICAL_PUBLICATION}",
                # Confirm flush every second so slot lag reflects decoding,
                # not pg_recvlogical's default 10 s feedback cadence.
                "-F", "1", "-s", "1", "-f", "-",
            ],
        )

    def _spawn(self) -> None:
        _docker_rm_force(self.name)
        self.proc = subprocess.Popen(
            self._argv(), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        threading.Thread(
            target=self._count_stdout, args=(self.proc,), daemon=True,
            name="logical-consumer-stdout",
        ).start()
        threading.Thread(
            target=self._scan_stderr, args=(self.proc,), daemon=True,
            name="logical-consumer-stderr",
        ).start()

    def _count_stdout(self, proc: subprocess.Popen) -> None:
        assert proc.stdout is not None
        while chunk := proc.stdout.read1(1 << 16):  # type: ignore[attr-defined]
            self.bytes_total += len(chunk)

    def _scan_stderr(self, proc: subprocess.Popen) -> None:
        assert proc.stderr is not None
        for raw in proc.stderr:
            line = raw.decode(errors="replace").strip()
            if not line:
                continue
            if "disconnected" in line:
                self.disconnects_total += 1
            elif "error" in line or "FATAL" in line:
                self.errors_total += 1
                self.last_error = line
            print(f"[recvlogical] {line}", file=sys.stderr)

    def start(self) -> None:
        self._spawn()
        self._monitor.start()

    def pause(self) -> None:
        subprocess.run(["docker", "pause", self.name], check=True, capture_output=True)
        self.paused = True

    def resume(self) -> None:
        if self.paused:
            subprocess.run(["docker", "unpause", self.name], check=True, capture_output=True)
            self.paused = False

    def _monitor_loop(self) -> None:
        while not self._stopping.wait(self.sample_every_s):
            proc = self.proc
            if proc is not None and proc.poll() is not None and not self._stopping.is_set():
                # pg_recvlogical retries lost connections itself; an exit is
                # a hard failure (slot gone, auth, …). Count it and restart,
                # as a supervised CDC sink would.
                self.exits_total += 1
                print(
                    f"[harness] logical consumer exited rc={proc.returncode}; "
                    f"restarting ({self.last_error})",
                    file=sys.stderr,
                )
                self._spawn()
            for metric, value in (
                ("logical_consumer_bytes_total", self.bytes_total),
                ("logical_consumer_disconnects_total", self.disconnects_total),
                ("logical_consumer_errors_total", self.errors_total),
                ("logical_consumer_exits_total", self.exits_total),
                ("logical_consumer_paused", 1 if self.paused else 0),
            ):
                self.emit("logical_consumer", LOGICAL_SLOT, metric, float(value))

    def stop(self) -> None:
        self._stopping.set()
        _docker_rm_force(self.name)
        if self.proc is not None:
            try:
                self.proc.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if self._monitor.is_alive():
            self._monitor.join(timeout=self.sample_every_s + 2.0)


def _drop_logical_objects(conn: psycopg.Connection) -> None:
    deadline = time.time() + 15.0
    while True:
        row = conn.execute(
            "SELECT active_pid FROM pg_replication_slots WHERE slot_name = %s",
            (LOGICAL_SLOT,),
        ).fetchone()
        if row is None:
            break
        if row[0] is not None:
            conn.execute("SELECT pg_terminate_backend(%s)", (row[0],))
        try:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (LOGICAL_SLOT,))
            break
        except psycopg.errors.ObjectInUse:
            if time.time() > deadline:
                raise
            time.sleep(0.5)
    conn.execute(f'DROP PUBLICATION IF EXISTS "{LOGICAL_PUBLICATION}"')


def _ensure_logical_replication(runtime: PhaseRuntime) -> LogicalConsumer:
    existing = runtime.state.get("logical-consumer")
    if existing is not None:
        return existing  # type: ignore[return-value]
    db_url = str(runtime.state.get("system_database_url") or runtime.database_url)
    db_name = str(runtime.state.get("system_database_name") or "")
    with psycopg.connect(db_url, autocommit=True) as conn:
        wal_level = conn.execute("SHOW wal_level").fetchone()[0]
        if wal_level != "logical":
            raise RuntimeError(
                f"{runtime.phase.type.value} needs wal_level=logical, server has "
                f"{wal_level!r}. The harness passes BENCH_PG_WAL_LEVEL=logical "
                "to compose when such a phase is scheduled; check the compose "
                "command for the selected engine."
            )
        _drop_logical_objects(conn)
        # publication=none: an empty publication. The slot still decodes all
        # WAL and pins catalog_xmin, but no table is published, isolating
        # slot effects from replica-identity failures in the queue.
        scope = runtime.phase.param("publication", "all")
        if scope not in ("all", "none"):
            raise ValueError(
                f"phase {runtime.phase.label!r}: publication must be all|none, "
                f"got {scope!r}"
            )
        conn.execute(
            f'CREATE PUBLICATION "{LOGICAL_PUBLICATION}"'
            + (" FOR ALL TABLES" if scope == "all" else "")
        )
        unreplicable = [
            row[0]
            for row in conn.execute(
                f"SELECT pt.schemaname || '.' || pt.tablename {PUBLISHED_TABLES_FROM}"
                f" WHERE p.pubname = %s AND {NO_REPLICA_IDENTITY_PREDICATE} ORDER BY 1",
                (LOGICAL_PUBLICATION,),
            ).fetchall()
        ]
        if unreplicable:
            print(
                f"[harness] WARNING: {len(unreplicable)} published table(s) have "
                "no replica identity; UPDATE/DELETE on them will fail while "
                f"{LOGICAL_PUBLICATION} exists: {', '.join(unreplicable)}",
                file=sys.stderr,
            )
        # Blocks until in-flight transactions finish (consistent snapshot).
        conn.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (LOGICAL_SLOT,),
        )
    consumer = LogicalConsumer(
        database_name=db_name,
        emit=_emit(runtime.state),
        sample_every_s=float(runtime.state.get("sample_every_s", 5)),
    )
    consumer.start()
    runtime.state["logical-consumer"] = consumer
    _wait_for_slot_active(db_url, timeout_s=60.0)
    return consumer


def _wait_for_slot_active(db_url: str, *, timeout_s: float) -> None:
    deadline = time.time() + timeout_s
    with psycopg.connect(db_url, autocommit=True) as conn:
        while time.time() < deadline:
            row = conn.execute(
                "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                (LOGICAL_SLOT,),
            ).fetchone()
            if row and row[0]:
                return
            time.sleep(0.5)
    raise RuntimeError(
        f"logical consumer did not attach to slot {LOGICAL_SLOT} "
        f"within {timeout_s:.0f}s"
    )


def enter_logical_stream(runtime: PhaseRuntime) -> None:
    _ensure_logical_replication(runtime).resume()


def enter_logical_stall(runtime: PhaseRuntime) -> None:
    _ensure_logical_replication(runtime).pause()


def exit_logical_stall(runtime: PhaseRuntime) -> None:
    consumer = runtime.state.get("logical-consumer")
    if consumer is not None:
        consumer.resume()  # type: ignore[attr-defined]


def teardown_logical_replication(state: dict[str, object]) -> None:
    consumer = state.pop("logical-consumer", None)
    if consumer is None:
        return
    consumer.resume()  # type: ignore[attr-defined]
    consumer.stop()  # type: ignore[attr-defined]
    db_url = str(state.get("system_database_url") or "")
    with psycopg.connect(db_url, autocommit=True) as conn:
        _drop_logical_objects(conn)
