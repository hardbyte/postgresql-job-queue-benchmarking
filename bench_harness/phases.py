"""Phase DSL: label=type:duration.

Phase types register (a) a tint for the plotter and (b) runtime enter/exit
hooks — but the plot tint is always available even when the runtime hooks
haven't been installed (useful for rendering from a fixture CSV without a
live Postgres).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Callable


class PhaseType(str, Enum):
    WARMUP = "warmup"
    CLEAN = "clean"
    IDLE_IN_TX = "idle-in-tx"
    # Near-zero offered load with the worker/maintenance runtime still
    # running — measures the queue engine's OWN background cost (WAL,
    # destructive-DDL churn, background xacts). Distinct from idle-in-tx,
    # which holds a transaction open to pin the vacuum horizon under load.
    IDLE_BACKGROUND = "idle-background"
    RECOVERY = "recovery"
    ACTIVE_READERS = "active-readers"
    HIGH_LOAD = "high-load"
    # Destructive / lifecycle phases. See UNIFIED_DRIVER_DESIGN.md §2.
    KILL_WORKER = "kill-worker"
    START_WORKER = "start-worker"
    # Chaos phase types (folded in from chaos.py — see issue #13).
    POSTGRES_RESTART = "postgres-restart"
    PG_BACKEND_KILL = "pg-backend-kill"
    POOL_EXHAUSTION = "pool-exhaustion"
    REPEATED_KILL = "repeated-kill"
    # Neighbour-impact phase types. neighbour-oltp runs a rate-limited
    # pgbench workload against a separate database on the same server for
    # the phase. logical-stream / logical-stall drive a logical replication
    # slot + consumer on the system's database; the slot persists from the
    # first logical phase until the system's run ends.
    NEIGHBOUR_OLTP = "neighbour-oltp"
    LOGICAL_STREAM = "logical-stream"
    LOGICAL_STALL = "logical-stall"
    # Workload-shape phases driven through adapter control files.
    RETRY_STORM = "retry-storm"
    SCHEDULE_PRELOAD = "schedule-preload"
    # CDC-suite phase types (docs/cdc-harness-design.md §9). Consumer-level
    # chaos is applied through the receiver's control API, not the replica
    # pool — the hooks live in cdc_harness, not bench_harness.hooks.
    CONSUMER_DEAD = "consumer-dead"
    CONSUMER_SLOW = "consumer-slow"
    SINK_OUTAGE = "sink-outage"
    BIG_TX = "big-tx"
    DDL_CHANGE = "ddl-change"
    SLOT_INVALIDATION = "slot-invalidation"


# Matplotlib-compatible colour tints. Tuned for the dark "neutral gray" base
# used by the rest of the plots; idle-in-tx is the scream colour.
PHASE_TINTS: dict[PhaseType, tuple[str, float]] = {
    PhaseType.WARMUP:          ("#D0D0D0", 0.40),
    PhaseType.CLEAN:           ("#B8B8B8", 0.35),
    PhaseType.IDLE_IN_TX:      ("#D46A6A", 0.30),
    PhaseType.IDLE_BACKGROUND: ("#6C8EBF", 0.25),
    PhaseType.RECOVERY:        ("#DCDCDC", 0.30),
    PhaseType.ACTIVE_READERS:  ("#E0B66C", 0.30),
    PhaseType.HIGH_LOAD:       ("#A378C8", 0.30),
    # Destructive: kill is scream-red, start is calm-green.
    PhaseType.KILL_WORKER:     ("#C04A4A", 0.35),
    PhaseType.START_WORKER:    ("#6CAF6C", 0.25),
    # Chaos: shades of red/orange to flag stress phases visually.
    PhaseType.POSTGRES_RESTART: ("#A03030", 0.40),
    PhaseType.PG_BACKEND_KILL:  ("#D86A3A", 0.30),
    PhaseType.POOL_EXHAUSTION:  ("#C8884A", 0.30),
    PhaseType.REPEATED_KILL:    ("#B04040", 0.35),
    PhaseType.NEIGHBOUR_OLTP:   ("#5FA8A0", 0.25),
    PhaseType.LOGICAL_STREAM:   ("#7FA6D8", 0.25),
    PhaseType.LOGICAL_STALL:    ("#D8A03A", 0.30),
    PhaseType.RETRY_STORM:      ("#C86A8A", 0.30),
    PhaseType.SCHEDULE_PRELOAD: ("#6CAFAF", 0.25),
    PhaseType.CONSUMER_DEAD:    ("#C04A4A", 0.35),
    PhaseType.CONSUMER_SLOW:    ("#D8A03A", 0.30),
    PhaseType.SINK_OUTAGE:      ("#A03030", 0.40),
    PhaseType.BIG_TX:           ("#A378C8", 0.35),
    PhaseType.DDL_CHANGE:       ("#6C8FAF", 0.30),
    PhaseType.SLOT_INVALIDATION: ("#802020", 0.45),
}

# Whether samples in this phase type feed into summary.json (warmup excluded).
# Destructive phases are *included* — the whole point is to capture the system
# behaviour during and after the destructive action.
PHASE_INCLUDED_IN_SUMMARY: dict[PhaseType, bool] = {
    PhaseType.WARMUP:          False,
    PhaseType.CLEAN:           True,
    PhaseType.IDLE_IN_TX:      True,
    PhaseType.IDLE_BACKGROUND: True,
    PhaseType.RECOVERY:        True,
    PhaseType.ACTIVE_READERS:  True,
    PhaseType.HIGH_LOAD:       True,
    PhaseType.KILL_WORKER:     True,
    PhaseType.START_WORKER:    True,
    PhaseType.POSTGRES_RESTART: True,
    PhaseType.PG_BACKEND_KILL:  True,
    PhaseType.POOL_EXHAUSTION:  True,
    PhaseType.REPEATED_KILL:    True,
    PhaseType.NEIGHBOUR_OLTP:   True,
    PhaseType.LOGICAL_STREAM:   True,
    PhaseType.LOGICAL_STALL:    True,
    PhaseType.RETRY_STORM:      True,
    PhaseType.SCHEDULE_PRELOAD: True,
    PhaseType.CONSUMER_DEAD:    True,
    PhaseType.CONSUMER_SLOW:    True,
    PhaseType.SINK_OUTAGE:      True,
    PhaseType.BIG_TX:           True,
    PhaseType.DDL_CHANGE:       True,
    PhaseType.SLOT_INVALIDATION: True,
}


@dataclass(frozen=True)
class Phase:
    label: str
    type: PhaseType
    duration_s: int
    # Type-specific parameters parsed from the spec's optional parenthesised
    # clause. For `kill-worker(instance=0)`, params == {"instance": "0"}.
    # Frozen dict-of-str-to-str: enough for every destructive phase type in
    # §2 of the design. A richer schema (ints, lists) can be layered on in
    # a future iteration; today instance indexing is the only consumer.
    params: tuple[tuple[str, str], ...] = ()

    def describe(self) -> str:
        minutes = self.duration_s / 60
        if minutes >= 1:
            pretty = f"{minutes:g}m"
        else:
            pretty = f"{self.duration_s}s"
        return f"{self.label} · {pretty}"

    def param(self, name: str, default: str | None = None) -> str | None:
        for k, v in self.params:
            if k == name:
                return v
        return default

    def float_param(self, name: str, default: float) -> float:
        raw = self.param(name)
        if raw is None:
            return default
        try:
            return float(raw)
        except ValueError as exc:
            raise ValueError(
                f"phase {self.label!r}: param {name!r} must be a number, "
                f"got {raw!r}"
            ) from exc

    def int_param(self, name: str, default: int) -> int:
        raw = self.param(name)
        if raw is None:
            return default
        try:
            return int(raw)
        except ValueError as exc:
            raise ValueError(
                f"phase {self.label!r}: param {name!r} must be an integer, "
                f"got {raw!r}"
            ) from exc


_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")


def parse_duration(text: str) -> int:
    """Parse a duration like '30s', '10m', '2h', '90' (seconds)."""
    text = text.strip().lower()
    if not text:
        raise ValueError("empty duration")
    unit_map = {"s": 1, "m": 60, "h": 3600}
    if text[-1] in unit_map:
        n = float(text[:-1])
        return int(n * unit_map[text[-1]])
    return int(text)


_TYPE_WITH_PARAMS_RE = re.compile(
    r"^(?P<type>[a-z][a-z0-9-]*)"  # type name, kebab-case
    r"(?:\((?P<params>[^)]*)\))?$"  # optional `(k=v,k=v)` params
)


def _parse_type_and_params(type_str: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    """Split `kill-worker(instance=0,signal=term)` into ("kill-worker",
    (("instance","0"),("signal","term"))). Legacy `idle-in-tx` parses to
    ("idle-in-tx", ()).
    """
    m = _TYPE_WITH_PARAMS_RE.match(type_str.strip())
    if not m:
        raise ValueError(
            f"Bad phase type spec {type_str!r}; expected "
            "<type> or <type>(key=value[,key=value])"
        )
    raw_params = m.group("params") or ""
    params: list[tuple[str, str]] = []
    if raw_params:
        for kv in raw_params.split(","):
            if "=" not in kv:
                raise ValueError(
                    f"Bad param in phase type {type_str!r}: expected k=v, got {kv!r}"
                )
            k, v = kv.split("=", 1)
            k = k.strip()
            v = v.strip()
            if not k:
                raise ValueError(f"Bad param in phase type {type_str!r}: empty key")
            params.append((k, v))
    return m.group("type"), tuple(params)


def parse_phase_spec(spec: str) -> Phase:
    """Parse a single --phase argument: label=type[(k=v,...)]:duration.

    Examples:
        warmup_1=warmup:10m            — no params
        kill=kill-worker(instance=0):60s — instance selector for the
                                           destructive action
    """
    if "=" not in spec or ":" not in spec:
        raise ValueError(
            f"Bad --phase spec {spec!r}; expected label=type:duration, "
            f"e.g. idle_1=idle-in-tx:60m"
        )
    label, rest = spec.split("=", 1)
    # Duration is always the last `:`-separated segment. We rsplit on `:`
    # so `kill-worker(instance=0):60s` — whose params section never
    # contains `:` — splits cleanly at the duration boundary.
    type_str, duration_str = rest.rsplit(":", 1)
    label = label.strip()
    if not _LABEL_RE.match(label):
        raise ValueError(
            f"Bad phase label {label!r}: must match [A-Za-z][A-Za-z0-9_]*"
        )
    type_name, params = _parse_type_and_params(type_str)
    try:
        phase_type = PhaseType(type_name)
    except ValueError as exc:
        raise ValueError(
            f"Unknown phase type {type_name!r}. "
            f"Known: {', '.join(t.value for t in PhaseType)}"
        ) from exc
    duration_s = parse_duration(duration_str)
    if duration_s <= 0:
        raise ValueError(f"Phase duration must be positive, got {duration_s}s")
    return Phase(label=label, type=phase_type, duration_s=duration_s, params=params)


# Named scenarios desugar into explicit phase lists.
SCENARIOS: dict[str, list[str]] = {
    # Idle / low-throughput background cost. Invoke with --producer-rate 0
    # (or a small trickle): the worker/maintenance runtime keeps ticking
    # while ~no jobs are offered, so the Postgres-side metrics (WAL bytes/s,
    # relfilenode/TRUNCATE churn, background xacts) isolate the queue
    # engine's own maintenance cost. Contrast idle_in_tx_saturation, which
    # pins the xmin horizon under load.
    "idle_background_cost": [
        "warmup=warmup:2m",
        "idle_1=idle-background:60m",
    ],
    "idle_in_tx_saturation": [
        "warmup=warmup:10m",
        "clean_1=clean:60m",
        "idle_1=idle-in-tx:60m",
        "recovery_1=recovery:30m",
    ],
    "long_horizon": [
        "warmup=warmup:10m",
        "clean_1=clean:60m",
        "idle_1=idle-in-tx:60m",
        "recovery_1=recovery:120m",
        "idle_2=idle-in-tx:120m",
    ],
    # Composition examples; shorter so manual runs stay tractable.
    "sustained_high_load": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "pressure_1=high-load:120m",
        "recovery_1=clean:30m",
    ],
    "active_readers": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "readers_1=active-readers:60m",
        "recovery_1=clean:30m",
    ],
    # Broad, balanced event/message-delivery comparison profile.
    # Starts at steady-state, adds subscriber/read pressure, then pushes
    # backlog pressure with high-load before finishing in a clean tail.
    "event_delivery_matrix": [
        "warmup=warmup:10m",
        "clean_1=clean:20m",
        "readers_1=active-readers:20m",
        "pressure_1=high-load:20m",
        "recovery_1=clean:20m",
    ],
    # Message-bus burst / catch-up profile. Use to compare how systems
    # absorb a sustained oversupply of work and how quickly they drain
    # once offered load returns to baseline.
    "event_delivery_burst": [
        "warmup=warmup:10m",
        "clean_1=clean:15m",
        "pressure_1=high-load:45m",
        "recovery_1=clean:30m",
    ],
    # Multi-replica steady-state comparison. Intended to be run with
    # `--replicas >= 2`; the phase sequence itself is nondestructive.
    "fleet_steady_state": [
        "warmup=warmup:10m",
        "clean_1=clean:30m",
        "readers_1=active-readers:30m",
    ],
    "soak": [
        "warmup=warmup:10m",
        "clean_1=clean:6h",
    ],
    # Mixed-queue scenario. Pair with `BENCH_QUEUE_COUNT=N` (default 4
    # if not set, but explicit on the command line is clearer). The
    # producer round-robins inserts across N queues, the consumer
    # registers N queue / consumer / subconsumer triples. Two things
    # we want to measure here: peak per-queue isolation (no per-queue
    # starvation under fair load) and the engine's per-queue overhead
    # (does N queues × baseline ≈ 1 queue × N×baseline, or do shared
    # writers serialise?).
    "mixed_queue": [
        "warmup=warmup:30s",
        "clean_1=clean:5m",
    ],
    # Replaces the legacy chaos.py `scenario_crash_recovery`. Harsh kill
    # of replica 0, then restart and measure recovery. Pass/fail answers
    # (`jobs_lost`, `recovery_time`) are derived from the shared raw.csv
    # time series — see UNIFIED_DRIVER_DESIGN.md §5.
    #
    # Meaningful only with `--replicas >=2`; the kill of replica 0 lets
    # replica 1 carry the load through the kill phase. Single-replica
    # runs technically work but the "recovery" phase simply measures
    # time-to-empty after restart.
    "crash_recovery": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "kill=kill-worker(instance=0):60s",
        "restart=start-worker(instance=0):60s",
    ],
    # Crash a replica while the fleet is already under backlog pressure, then
    # restart it and watch the tail recover. Intended for --replicas >= 2.
    "crash_recovery_under_load": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "pressure_1=high-load:120s",
        "kill=kill-worker(instance=0):60s",
        "restart=start-worker(instance=0):60s",
        "recovery_1=clean:120s",
    ],
    # ── Chaos scenarios (folded in from chaos.py — see issue #13) ──────
    # Each scenario follows the same warmup→baseline→stress→recovery
    # shape, so the per-phase aggregator can diff enqueue/completion
    # cumulatives across the stress + recovery span to surface
    # jobs_lost / recovery_time in summary.json.
    "chaos_crash_recovery": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "kill=kill-worker(instance=0):60s",
        "restart=start-worker(instance=0):60s",
        "recovery=clean:60s",
    ],
    "chaos_postgres_restart": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "restart=postgres-restart:60s",
        "recovery=clean:60s",
    ],
    "chaos_repeated_kills": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "repeated=repeated-kill(instance=0,period=20s):120s",
        "recovery=clean:60s",
    ],
    "chaos_pg_backend_kill": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "kills=pg-backend-kill(rate=2):60s",
        "recovery=clean:60s",
    ],
    "chaos_pool_exhaustion": [
        "warmup=warmup:30s",
        "baseline=clean:60s",
        "exhaustion=pool-exhaustion(idle_conns=300):60s",
        "recovery=clean:60s",
    ],
    # Does the queue starve unrelated OLTP on the same server? A fixed-rate
    # pgbench neighbour runs in every phase; only the queue's offered load
    # (`load` × --producer-rate) changes. baseline holds the producer at
    # zero with workers still running.
    "neighbour_oltp": [
        "warmup=warmup:5m",
        "baseline=neighbour-oltp(load=0):10m",
        "moderate=neighbour-oltp(load=1):15m",
        "saturation=neighbour-oltp(load=4):15m",
        "recovery=neighbour-oltp(load=1):15m",
    ],
    # Does the queue coexist with logical replication? clean_1 has no slot
    # (wal_level=logical overhead only); stream_1 adds a FOR ALL TABLES
    # publication, a pgoutput slot and a streaming consumer, which persist
    # for the rest of the run. stall_1 is the "CDC sink is down" case:
    # retained WAL and catalog bloat grow until the consumer resumes.
    "logical_replication": [
        "warmup=warmup:5m",
        "clean_1=clean:10m",
        "stream_1=logical-stream:15m",
        "pressure_1=high-load:15m",
        "stall_1=logical-stall:15m",
        "catchup_1=logical-stream:15m",
    ],
    # Scheduled-job thundering herd. The preload phase enqueues `count`
    # jobs all due at the end of the phase (delay defaults to the phase
    # duration) while the steady immediate stream keeps flowing; `due`
    # observes the herd coming due and draining. Adapters report
    # schedule_lateness_* for the herd; claim_* stays immediate-only.
    "scheduled_burst": [
        "warmup=warmup:5m",
        "baseline=clean:5m",
        "preload=schedule-preload(count=100000):5m",
        "due=clean:10m",
        "after=clean:5m",
    ],
    # Steady promotion accuracy: the herd is spread uniformly over a
    # window instead of sharing one instant.
    "scheduled_spread": [
        "warmup=warmup:5m",
        "preload=schedule-preload(count=60000,delay=2m,spread=10m):2m",
        "spread=clean:10m",
        "after=clean:5m",
    ],
    # Failure churn: a share of jobs fail transiently and retry with each
    # system's own backoff, a small share are poison (always fail until
    # max attempts), then injection stops and the retry backlog drains.
    # Runs with JOB_MAX_ATTEMPTS=5 unless the environment overrides it.
    "retry_storm": [
        "warmup=warmup:5m",
        "baseline=clean:10m",
        "storm=retry-storm(transient_pct=30,poison_pct=1):20m",
        "recovery=recovery:20m",
    ],
}

# Phase types whose hooks create a logical replication slot. The harness
# starts Postgres with wal_level=logical when any of them is scheduled.
LOGICAL_REPLICATION_PHASE_TYPES = frozenset(
    {PhaseType.LOGICAL_STREAM, PhaseType.LOGICAL_STALL}
)


def required_wal_level(phases: list[Phase]) -> str:
    if any(p.type in LOGICAL_REPLICATION_PHASE_TYPES for p in phases):
        return "logical"
    return "replica"


def resolve_scenario(
    scenario: str | None,
    extra_phases: list[str] | None,
) -> list[Phase]:
    specs: list[str] = []
    if scenario is not None:
        if scenario not in SCENARIOS:
            raise ValueError(
                f"Unknown scenario {scenario!r}. "
                f"Known: {', '.join(SCENARIOS)}"
            )
        specs.extend(SCENARIOS[scenario])
    if extra_phases:
        specs.extend(extra_phases)
    if not specs:
        raise ValueError("no phases supplied (use --scenario or --phase)")
    phases = [parse_phase_spec(s) for s in specs]
    labels_seen: set[str] = set()
    for phase in phases:
        if phase.label in labels_seen:
            raise ValueError(f"Duplicate phase label: {phase.label!r}")
        labels_seen.add(phase.label)
    if phases[0].type is not PhaseType.WARMUP:
        raise ValueError(
            "First phase must be type warmup so samples can be excluded "
            "from summaries; got "
            f"{phases[0].type.value}"
        )
    return phases


# ────────────────────────────────────────────────────────────────────────
# Runtime hooks
# ────────────────────────────────────────────────────────────────────────
#
# Phase-type hooks are registered as a pair of (enter, exit) callables that
# take a live context object (the harness passes in a PhaseRuntime with a
# DB URL + adapter handle + logger). They can open side connections, raise
# producer rates on the adapter, etc.
#
# The functions here don't need a live database to import — real work happens
# inside the callables, which are imported by the orchestrator only.

PhaseHook = Callable[["PhaseRuntime"], None]


@dataclass
class PhaseRuntime:
    """Passed to phase enter/exit hooks."""
    database_url: str
    phase: Phase
    # Opaque state the hook may stash for its exit pair.
    state: dict[str, object]


RunHook = Callable[[dict[str, object]], None]


class HookRegistry:
    def __init__(self) -> None:
        self._enter: dict[PhaseType, PhaseHook] = {}
        self._exit: dict[PhaseType, PhaseHook] = {}
        self._prepare: dict[PhaseType, RunHook] = {}
        self._teardown: dict[PhaseType, RunHook] = {}

    def register(
        self,
        phase_type: PhaseType,
        enter: PhaseHook | None = None,
        exit: PhaseHook | None = None,
        prepare: RunHook | None = None,
        teardown: RunHook | None = None,
    ) -> None:
        """`prepare` runs once per system before the first phase when the
        phase list contains `phase_type`; `teardown` runs once after the
        last phase (also on abort). Both receive the shared phase state."""
        if enter:
            self._enter[phase_type] = enter
        if exit:
            self._exit[phase_type] = exit
        if prepare:
            self._prepare[phase_type] = prepare
        if teardown:
            self._teardown[phase_type] = teardown

    def _run_hooks_for(
        self,
        hooks: dict[PhaseType, RunHook],
        phases: list[Phase],
        state: dict[str, object],
    ) -> None:
        seen: set[RunHook] = set()
        for phase in phases:
            hook = hooks.get(phase.type)
            if hook and hook not in seen:
                seen.add(hook)
                hook(state)

    def prepare(self, phases: list[Phase], state: dict[str, object]) -> None:
        self._run_hooks_for(self._prepare, phases, state)

    def teardown(self, phases: list[Phase], state: dict[str, object]) -> None:
        self._run_hooks_for(self._teardown, phases, state)

    def enter(self, runtime: PhaseRuntime) -> None:
        hook = self._enter.get(runtime.phase.type)
        if hook:
            hook(runtime)

    def exit(self, runtime: PhaseRuntime) -> None:
        hook = self._exit.get(runtime.phase.type)
        if hook:
            hook(runtime)


def default_registry() -> HookRegistry:
    """Build the default registry, wiring in the phase-type hooks.

    Importing here avoids forcing psycopg at import-time for harness users
    who only want the DSL (e.g. the CI smoke test).
    """
    from . import hooks  # local to avoid eager psycopg import
    registry = HookRegistry()
    registry.register(PhaseType.IDLE_IN_TX,
                      enter=hooks.enter_idle_in_tx,
                      exit=hooks.exit_idle_in_tx)
    registry.register(PhaseType.ACTIVE_READERS,
                      enter=hooks.enter_active_readers,
                      exit=hooks.exit_active_readers)
    registry.register(PhaseType.HIGH_LOAD,
                      enter=hooks.enter_high_load,
                      exit=hooks.exit_high_load)
    # Destructive phase types act on the replica pool via
    # state["replica_pool"] — see hooks.enter_kill_worker / enter_start_worker.
    registry.register(PhaseType.KILL_WORKER,
                      enter=hooks.enter_kill_worker)
    registry.register(PhaseType.START_WORKER,
                      enter=hooks.enter_start_worker)
    # Chaos phase types — folded in from chaos.py. See hooks.py.
    registry.register(PhaseType.POSTGRES_RESTART,
                      enter=hooks.enter_postgres_restart,
                      exit=hooks.exit_postgres_restart)
    registry.register(PhaseType.PG_BACKEND_KILL,
                      enter=hooks.enter_pg_backend_kill,
                      exit=hooks.exit_pg_backend_kill)
    registry.register(PhaseType.POOL_EXHAUSTION,
                      enter=hooks.enter_pool_exhaustion,
                      exit=hooks.exit_pool_exhaustion)
    registry.register(PhaseType.REPEATED_KILL,
                      enter=hooks.enter_repeated_kill,
                      exit=hooks.exit_repeated_kill)
    registry.register(PhaseType.NEIGHBOUR_OLTP,
                      enter=hooks.enter_neighbour_oltp,
                      exit=hooks.exit_neighbour_oltp,
                      prepare=hooks.prepare_neighbour_oltp)
    # Both logical phase types share one slot/consumer whose lifetime is
    # the whole system run, so they share prepare/teardown.
    registry.register(PhaseType.LOGICAL_STREAM,
                      enter=hooks.enter_logical_stream,
                      teardown=hooks.teardown_logical_replication)
    registry.register(PhaseType.LOGICAL_STALL,
                      enter=hooks.enter_logical_stall,
                      exit=hooks.exit_logical_stall,
                      teardown=hooks.teardown_logical_replication)
    registry.register(PhaseType.RETRY_STORM,
                      enter=hooks.enter_retry_storm,
                      exit=hooks.exit_retry_storm)
    registry.register(PhaseType.SCHEDULE_PRELOAD,
                      enter=hooks.enter_schedule_preload)
    # warmup, clean, recovery — no extra runtime action; the adapter's
    # steady workload carries the load.
    return registry
