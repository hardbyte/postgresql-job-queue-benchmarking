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
| `queue_fanout` | Clean load then an idle tail (`rate=0`). Run twice at the same `--producer-rate`, once with `--queue-count 1` and once with e.g. `--queue-count 500`, and compare backends, LISTEN connections, xacts/s, CPU and NOTIFY load. Exposes per-queue connections and per-queue polling. |
| `large_payload` | Clean load then a drain tail. Run once per size with `--job-payload-bytes 16384` / `65536` / `262144` and `--job-payload-kind random` at a moderate rate (e.g. `--producer-rate 100`). Reports WAL bytes per job, TOAST size and TOAST bloat, and table size once the queue is empty again. |
| `long_jobs` | 500 jobs preloaded behind the consumer gate, then released together (`hold`: all running, none can finish yet), then continuous long jobs (`steady`) and a drain. Pair with `--job-work-ms 120000`, >= 500 total workers (e.g. `--replicas 2 --worker-count 250`) and lease/timeout knobs above the job duration (see below). Measures heartbeat/lease write amplification and duplicate execution. |
| `backlog_drain` | Preload 1M jobs with consumers gated, drain at full speed, then idle. Reports drain time, throughput deciles across the drain, table/index size before and after, dead tuples, autovacuum runs and how long the database takes to return to its pre-load size. Leave `--producer-rate` > 0; the scenario sets every phase's rate itself. |

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

## Workload-shape flags

| Flag | Adapter env | Effect |
|---|---|---|
| `--job-payload-bytes N` | `JOB_PAYLOAD_BYTES` | Approximate job payload size (adapter default 256). |
| `--job-payload-kind random` | `JOB_PAYLOAD_KIND=random` | Pads with incompressible base64 noise instead of `x`s, so TOAST compression can't hide the size. Also makes procrastinate pad its jobs (it ignores `JOB_PAYLOAD_BYTES` otherwise). |
| `--job-work-ms N` | `JOB_WORK_MS` | Synthetic per-job work time (adapter default 1 ms). |
| `--queue-count N` | `BENCH_QUEUE_COUNT`, `BENCH_DEPTH_ROTATE` | N logical queues per adapter; worker concurrency is split evenly (min 1 per queue). |

`--queue-count` support:

| System | N > 1 behaviour |
|---|---|
| awa | One `Client::queue()` per queue. Each queue's dispatcher holds its own `LISTEN` connection from the client's pool and polls on its own interval, so the pool (`MAX_CONNECTIONS`, adapter default `4 × WORKER_COUNT + 48`) must exceed the queue count. |
| river | One `QueueConfig` per queue in a single client; jobs spread per job. |
| oban | `Oban.start_queue/1` per queue; jobs spread per job. |
| procrastinate | One worker listening on all N queues; batches spread round-robin. |
| pg-boss | `createQueue` (partitioned) + `work()` per queue; batches spread round-robin. |
| pgmq | One queue table pair per queue. pgmq has no cross-queue read, so each consumer polls its share of the queues in turn. |
| absurd | One queue (tables) and one `AsyncAbsurd` worker, with its own connection, per queue. |
| pgque | One consumer, ticker pass and `LISTEN` connection per queue (adapter design). |

Per-queue polling cost scales with the poll intervals the adapters pick
for single-queue latency: 50 ms for awa, river, pgmq and absurd, 500 ms
for pg-boss. Read idle `pg_db_xacts_per_s` at large N with that in mind.

For N > 1 the adapters' depth observers use a single statement across all
queues, or (awa, pgque) refresh one queue per tick, so observer load stays
flat as N grows.

Lease and timeout knobs forwarded from the harness environment, which
`long_jobs` needs set above the job duration to measure the system rather
than the adapter's defaults: `RESCUE_AFTER_SECS` (river job timeout and
stuck-job rescue, oban lifeline; default 30 s and 15 s), `VISIBILITY_TIMEOUT_S`
and `CONSUMER_BATCH_SIZE` (pgmq; default 30 s and >= 8, where each read
batch runs serially), `SUBSCRIBER_BATCH_SIZE` (pg-boss) and
`CLAIM_TIMEOUT_SECS` (absurd; default 10 s). awa's lease deadline is
`LEASE_DEADLINE_MS` (default 5 min).

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
| `preload(jobs=N)` | Offers N jobs in total, spread evenly over the phase and across replicas, while consumers are held behind the consumer gate. The harness only creates the gate when a run contains a `preload`. |
| `drain` | Producer stopped, consumer gate opened (one-shot: every `preload` must come before the first `drain`). |

Any phase also accepts `rate=N`, a per-replica jobs/s override that applies
for that phase only, e.g. `warmup=warmup(rate=0):1m` or
`idle_1=idle-background(rate=0):10m`. Rates reach adapter-paced producers
through the rate control file and harness-paced producers (awa, pgque)
through the pacer, which re-reads the same file, so `high-load`
takes effect for harness-paced adapters too. Tokens still buffered in the
adapter's stdin when the rate drops to 0 are discarded.

Adapter samples are attributed to the phase that is current when they
arrive, so per-phase job counts can include up to one sample period from
the neighbouring phase. Drain timing uses an exact phase-start marker.

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

Resource and scaling diagnostics, sampled every tick:

| Metric (`raw.csv`) | Source | Summary (`summary.json`, per phase) |
|---|---|---|
| `pg_backends_total` / `_active` / `_idle` / `_idle_in_tx` / `_listening` | `pg_stat_activity` client backends on the system database, excluding the harness's own connections. `listening` = backends whose last statement was `LISTEN`. | `median_/peak_pg_backends`, `median_/peak_pg_backends_listening` |
| `pg_db_tup_inserted_total` (plus the existing updated/deleted) | `pg_stat_database` | `pg_db_tup_{updated,inserted,deleted}_per_s`; with `pg_db_xacts_per_s` this is the polling and heartbeat write rate |
| `pg_cpu_seconds_total` | Postgres container cgroup `cpu.stat` | `pg_cpu_cores` |
| `adapter_cpu_seconds_total` (subject `replica-<i>`) | adapter container cgroup (via `docker run --cidfile`) or `/proc/<pid>/stat` | `adapter_cpu_cores` (sum over replicas) |
| `pg_database_size_mb` | `pg_database_size()` | `database_size_mb_{start,end,peak}` |
| `toast_size_mb`, `indexes_size_mb` (per event table) | `reltoastrelid`, `pg_indexes_size()` | `event_toast_size_mb_*`, `event_indexes_size_mb_*`, `event_relation_size_mb_*` |
| `toast_pgstattuple_dead_pct` / `_free_pct` | `pgstattuple` on each non-empty TOAST table, at phase boundaries | under `metrics` |
| `pg_stmt_calls_total`, `pg_stmt_notify_calls_total` | `pg_stat_statements` (opt-in) | `pg_stmt_calls_per_s`, `pg_notify_calls_per_s` |

Derived per phase: `jobs_enqueued` / `jobs_completed` (rate × window summed
over replicas) and `wal_bytes_per_job` (WAL delta / max of the two). Drain
phases get a `drain` block (`backlog_at_start`, `drain_time_s`,
`jobs_drained`, `mean_drain_rate_per_s`, `drain_throughput_deciles`,
`drain_tail_to_head_ratio`). Each system gets a `run` block:
`jobs_enqueued_total`, `jobs_completed_total`, `completion_excess` (> 0
means duplicate execution; a few jobs either way is sample-timing noise),
`database_size_mb_{baseline,peak,final}` and `size_settle_s` (time from the
first drain until the database is within max(10%, 8 MB) of its first
sample). The job totals miss jobs enqueued before an adapter's first sample
tick and completions after its last, so `completion_excess` is only a clean
duplicate-execution signal for runs that start with a `rate=0` warmup and
end with a drain, as `long_jobs` and `backlog_drain` do.

`pg_stat_statements` is off by default because preloading it changes the
server for every scenario. Set `BENCH_PG_STAT_STATEMENTS=1` to preload it
(`track = all`, so `pg_notify()` inside triggers and functions counts) and
install it in each system database. With `track = all`, statements run
inside functions count too, so `pg_stmt_calls_per_s` measures statement
work rather than client round trips; use `pg_db_xacts_per_s` for the
latter. The NOTIFY count matches statements
mentioning `notify` / `pg_notify`.

Wait-event output lands in `raw.csv` (`subject_kind=wait_event`),
`summary.json` (top-10 events per phase plus `total_active_samples`), and
a stacked bar plot per system in `index.html`. Wait-event sampling is on
by default at 1 s cadence; opt out with `--no-wait-events` or tune via
`--wait-event-sample-every <seconds>`. Primer with the common event
types and how to read the stack:
[`docs/wait-events.md`](./wait-events.md).
