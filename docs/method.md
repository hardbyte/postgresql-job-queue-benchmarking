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
| `neighbour_oltp` | Does the queue starve unrelated traffic on the same server? A fixed-rate pgbench neighbour (separate `neighbour` database, 8 clients, 300 TPS, TPC-B-like) runs through baseline (queue producer at 0, workers running) → moderate (1×) → saturation (4× `--producer-rate`) → recovery (1×). Headline: neighbour latency p50/p99 per phase and its ratio to baseline. |
| `logical_replication` | Does the queue coexist with logical replication? Starts Postgres with `wal_level=logical`. clean (no slot) → stream (FOR ALL TABLES publication + `pgoutput` slot + streaming consumer) → high-load with the slot streaming → stall (consumer frozen: the "CDC sink is down" case) → catch-up. Headline: slot lag, retained WAL, `catalog_xmin` age, catalog bloat, WAL per job, consumer disconnects, and published tables without a replica identity (UPDATE/DELETE on them fails while the publication exists). Retained WAL grows for the whole stall, so the 15-minute stall at high rates needs several GB of free disk on the Docker volume. |
| `mixed_queue` | Multi-queue run; pair with `BENCH_QUEUE_COUNT=N` to spawn N parallel queues. Producer round-robins inserts; consumer side registers N queue subscriptions. Tests per-queue isolation and engine-side per-queue overhead. |

## Awa tuning knobs

These environment variables are forwarded to the Awa adapter for focused
Awa-only experiments:

| Variable | Default | Effect |
|---|---:|---|
| `BENCH_QUEUE_COUNT` | `1` | Number of logical queues registered by one adapter process. Producer inserts round-robin across them; workers are divided evenly across queues. |
| `AWA_COMPLETION_SHARDS` | queue-storage: `1`; canonical: `8` | Completion batcher flush workers inside one adapter process. |
| `AWA_QUEUE_CLAIMERS` | `1` | Queue-storage dispatcher/claimer loops per logical queue. Claimers share that queue's worker permits. |
| `AWA_CLAIM_BATCH_SIZE` | `512` | Maximum jobs each claimer attempts to claim in one database round trip. |
| `AWA_QS_PRODUCER_PATH` | `copy` | Queue-storage producer entry point: `copy` (`enqueue_params_copy`) or `batch` (`enqueue_params_batch`). |

To benchmark an unpublished awa checkout, point the adapter at it with a
local Cargo patch (gitignored), then build as usual:

```toml
# awa-bench/.cargo/config.toml
[patch."https://github.com/hardbyte/awa"]
awa-model = { path = "/path/to/awa/awa-model" }
awa-worker = { path = "/path/to/awa/awa-worker" }
awa-macros = { path = "/path/to/awa/awa-macros" }
```

The patch rewrites `awa-bench/Cargo.lock`; restore it before committing.

Set `BENCH_PG_PORT` (default `15555`) to run a second harness against its own
Postgres container, e.g. from another worktree.

## Adapter version notes

Caveats from upstream version bumps that change benchmark semantics but
aren't visible from the version number alone:

- **absurd-bench**: `absurd-sdk` 0.4.0 fixed a bug where a task registered
  without an explicit `max_attempts` didn't correctly fall back to the
  `AsyncAbsurd` app's `default_max_attempts`. The adapter's
  `register_task(TASK_NAME)` call has never set `max_attempts` explicitly,
  so this bump silently changes its effective retry count from whatever the
  prior buggy fallback resolved to, to the correct `default_max_attempts=5`.
  No adapter code change needed, but keep this in mind when comparing
  retry/DLQ behavior against runs from before the 0.4.0 bump.

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
| `neighbour-oltp(load=X,clients=N,rate=R,script=S)` | Runs `pgbench -R R -c N` against the `neighbour` database for the phase, from a sidecar container using the stock Postgres image. The database is re-initialised with `pgbench -i -s 10` once per system, during warmup. Set the scale with `NEIGHBOUR_PGBENCH_SCALE`. `load` sets the producer rate to `X × --producer-rate` for the phase (default 1). `script` takes pgbench built-ins joined by `+`, e.g. `select-only@9+simple-update@1` (default `tpcb-like`). Latency is measured from each transaction's scheduled start, so it includes queueing. `service_p*` excludes schedule lag: high latency with normal service time means the neighbour fell behind its rate, and high service time means each transaction slowed. |
| `logical-stream(publication=all\|none)` | The first logical phase creates publication `bench_cdc_pub` (FOR ALL TABLES, or empty with `publication=none`), creates slot `bench_cdc_slot` (`pgoutput`) and starts a `pg_recvlogical` consumer that discards output, confirms every second and reconnects on loss. All three persist through later phases until the system's run ends. This phase type keeps the consumer streaming. Postgres is started with `wal_level=logical` when any logical phase is scheduled. |
| `logical-stall` | Same slot. The consumer container is frozen (`docker pause`) for this phase only, so the slot stops advancing. The walsender drops it after `wal_sender_timeout`, and the consumer reconnects when the phase ends. |

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

The metrics daemon also polls system-catalog health and logical slots every
tick. `subject_kind=catalog` carries `catalog_n_dead_tup` and
`catalog_size_mb` for `pg_class`, `pg_attribute`, `pg_depend` and `pg_type`.
That is where TRUNCATE rotation and partition churn show up.
`subject_kind=replication_slot` carries `slot_confirmed_lag_bytes`,
`slot_retained_wal_bytes`, `slot_catalog_xmin_age`, `slot_active` and the
`slot_decoded_bytes_total` / `slot_spill_bytes_total` counters.
`subject_kind=publication` carries
`publication_tables_without_replica_identity`. The logical consumer emits
`logical_consumer_*` counters, and pgbench emits `neighbour_*` 5 s-window
series plus exact per-phase `neighbour_phase_*` stats. `summary.json`
condenses these per phase into the `neighbour`, `logical` and `catalog`
blocks, plus `wal_bytes_per_completed_job` (cluster WAL rate ÷ mean
completion rate; it includes any neighbour workload's WAL). `index.html`
shows them in a "Postgres Neighbours" table.

Wait-event output lands in `raw.csv` (`subject_kind=wait_event`),
`summary.json` (top-10 events per phase plus `total_active_samples`), and
a stacked bar plot per system in `index.html`. Wait-event sampling is on
by default at 1 s cadence; opt out with `--no-wait-events` or tune via
`--wait-event-sample-every <seconds>`. Primer with the common event
types and how to read the stack:
[`docs/wait-events.md`](./wait-events.md).
