# Method

How the harness composes scenarios, what each phase type does, and the
Postgres-side diagnostics it captures. The README is the comparison; this
file is the reference.

## Scenarios

Each named scenario desugars to a phase sequence; pass `--scenario <name>`
to `bench.py run`, or compose your own with
`--phase <label>=<type>:<duration>`.

| Scenario | What it exercises |
|---|---|
| `idle_in_tx_saturation` | Steady-state baseline → an idle-in-transaction holder takes a writing tx with an XID assigned and pins the cluster xmin → recovery. The classic Postgres bloat trigger. Surfaces how a system holds up when autovacuum can't reclaim dead tuples. |
| `long_horizon` | Like `idle_in_tx_saturation` but longer, with a second idle-in-tx phase after recovery. Used for bloat-recovery soak studies. |
| `sustained_high_load` | Baseline → sustained 1.5× offered load → recovery. Tests whether the queue engine collapses or degrades gracefully when producers outpace workers. |
| `active_readers` | Baseline → 4 overlapping `REPEATABLE READ` connections running repeating scans against the queue's hot tables → recovery. Mirrors the analytics-on-OLTP pattern that pins MVCC horizon without an explicit idle-in-tx. |
| `event_delivery_matrix` | Balanced compare profile: clean → readers → high-load → recovery. The "broad shape comparison" scenario for cross-system dashboards. |
| `event_delivery_burst` | Burst / catch-up profile: clean → 45 min of high-load → 30 min recovery. Measures absorption + drain after a sustained oversupply of work. |
| `fleet_steady_state` | Multi-replica steady-state. Pair with `--replicas >= 2`. |
| `soak` | Warmup + 6 hours clean. Used to detect slow drift that shorter runs miss. |
| `crash_recovery` | Clean → SIGKILL replica 0 → restart → recovery. Pair with `--replicas >= 2` for a meaningful "fleet covers the kill" measurement; single-replica still works but the recovery phase just measures time-to-empty. |
| `crash_recovery_under_load` | `crash_recovery` with a high-load phase before the kill, so the fleet is already under backlog pressure. Pair with `--replicas >= 2`. |
| `chaos_crash_recovery` | Warmup → baseline → SIGKILL replica 0 → restart → recovery. Aggregates `jobs_lost` and `chaos_recovery_time_s` into `summary.json`. |
| `chaos_postgres_restart` | Stop + start the Postgres container mid-run; SUT must reconnect and drain. |
| `chaos_repeated_kills` | Periodic SIGKILL+restart of replica 0 across a sustained chaos phase. |
| `chaos_pg_backend_kill` | Steady stream of `pg_terminate_backend` against the SUT's connections. |
| `chaos_pool_exhaustion` | Hold 300 idle connections to pressure the SUT's pool sizing. |
| `mixed_queue` | Multi-queue run; pair with `BENCH_QUEUE_COUNT=N` to spawn N parallel queues. Producer round-robins inserts; consumer side registers N queue subscriptions. Tests per-queue isolation and engine-side per-queue overhead. |

## Phase types (compose your own)

| Phase type | What it does |
|---|---|
| `warmup` | Steady producer load for absorbing startup artifacts; samples are excluded from the summary. |
| `clean` | Steady-state baseline at the configured `--producer-rate`. |
| `high-load` | Steady producer load multiplied by `--high-load-multiplier` (default 1.5). |
| `idle-in-tx` | Opens one `BEGIN` + `SELECT txid_current()` connection that holds an XID for the whole phase. Simulates a long-running writing transaction (held xmin → vacuum starvation). |
| `active-readers` | Opens N (default 4, set via `ACTIVE_READER_COUNT`) `REPEATABLE READ` connections doing repeating scans against the adapter's hot tables. Simulates analytics readers on the OLTP path. |
| `recovery` | Producer load drops to baseline; the bench measures how the system catches up after a stress phase. |
| `kill-worker(instance=N)` | SIGKILLs replica N and waits for the configured duration. Used inside `crash_recovery` scenarios. |
| `start-worker(instance=N)` | Restarts a previously killed replica and watches for re-registration. |
| `postgres-restart` | `docker compose stop postgres` for half the duration, then `start` for the rest. Drives the harness-managed compose lifecycle. |
| `pg-backend-kill(rate=N)` | Opens an admin connection that runs `pg_terminate_backend(pid)` against the SUT's database `N` times per second. |
| `pool-exhaustion(idle_conns=N)` | Holds `N` idle connections against the SUT's database for the duration; releases them on phase end. |
| `repeated-kill(instance=I,period=Ns)` | Periodic SIGKILL + auto-restart of replica `I` every `period`. Composes `kill-worker` / `start-worker`. |

## Postgres diagnostics

Throughput, latency, and bloat answer *that* one system is slower than
another. **Wait events** answer *why* — the postgres-side reason a system
is bottlenecked, not just whether it is. The harness samples
`pg_stat_activity` once per second from a dedicated connection and
aggregates non-idle backend snapshots into a per-phase histogram of
`(wait_event_type, wait_event)`. Same shape as
[pg_ash](https://github.com/NikolayS/pg_ash) produces, implemented inside
the harness so we don't have to swap the postgres image.

The metrics daemon also records `pg_notification_queue_usage()` and
active transaction context from `pg_stat_activity` during load.
Notification queue usage lands in `raw.csv` as the cluster metric
`pg_notification_queue_usage`; active transaction rows land as
`subject_kind=pg_activity` with `xact_age_s` as the numeric value and the
backend pid, application name, state, `xact_start`, wait event, and
compacted query text encoded in the subject.

Wait-event output lands in `raw.csv` (`subject_kind=wait_event`),
`summary.json` (top-10 events per phase plus `total_active_samples`), and
a stacked bar plot per system in `index.html`. Wait-event sampling is on
by default at 1 s cadence; opt out with `--no-wait-events` or tune via
`--wait-event-sample-every <seconds>`. Primer with the common event
types and how to read the stack:
[`docs/wait-events.md`](./wait-events.md).
