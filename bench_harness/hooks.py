"""Phase-type enter/exit runtime hooks.

Each hook receives a PhaseRuntime and may stash state in runtime.state that
its paired exit hook retrieves to clean up.

Hooks run synchronously on the orchestrator thread — they should complete
fast. Long-running side effects (held transactions, background readers) get
forked into threads that block on an event the exit hook signals.
"""

from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from typing import Any

import psycopg

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
