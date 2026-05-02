# Wait-event sampling primer

Throughput and latency tell you *that* a system is slower than another.
Wait events tell you *why*: which postgres-side resource is the queue
spending time on. Same idea as Oracle's Active Session History (ASH) and
the [pg_ash](https://github.com/NikolayS/pg_ash) extension, implemented
inside the harness so we don't have to swap the postgres image.

## How it works

The orchestrator opens a dedicated Postgres connection and polls
`pg_stat_activity` once per second:

```sql
SELECT pid, state, wait_event_type, wait_event
FROM pg_stat_activity
WHERE pid <> pg_backend_pid()
  AND state IS NOT NULL
  AND state <> 'idle';
```

Each row is a backend that was *active* (running a query, in a
transaction, or waiting on a resource) at sample time. We bucket the
rows into a `Counter[(wait_event_type, wait_event)]` per phase. Phase
boundaries drain the counter into `raw.csv`:

- `subject_kind = wait_event`
- `subject = "<event_type>:<event_name>"` (e.g. `Lock:tuple`,
  `IO:DataFileRead`, `LWLock:WALWrite`, `CPU:CPU`)
- `metric = wait_event_count`, `value = sample count`
- A companion row with `metric = total_active_samples` carries the
  denominator so callers can compute percentages without re-summing.

`pg_stat_activity` is a public catalog view. No `SECURITY DEFINER`,
no superuser, no extension install — runs against the standard
`postgres:17.2-alpine` image.

## Reading the histogram

`wait_event_type` is the coarse bucket. The ones you'll see most often
in a job-queue workload:

| Type        | What it means                                                                 |
| ----------- | ----------------------------------------------------------------------------- |
| `CPU`       | Synthetic bucket the harness uses for backends with no current wait. On-CPU. |
| `Lock`      | Heavyweight lock contention (row, tuple, transaction, advisory).             |
| `LWLock`    | Lightweight lock contention (buffers, WAL, locks themselves).                |
| `IO`        | Disk reads / writes / fsync.                                                 |
| `Client`    | Waiting on the client (`ClientRead` after a query returns, idle-in-tx).      |
| `IPC`       | Inter-backend coordination (parallel query, replication).                    |
| `BufferPin` | Concurrent reader holding a buffer pin.                                      |
| `Activity`  | Background workers waiting for work (autovacuum launcher, archiver, etc.).   |

`wait_event` is the fine-grained subevent — exact docs at
<https://www.postgresql.org/docs/current/monitoring-stats.html#WAIT-EVENT-TABLE>.

A few specific events to pattern-match against the job-queue systems we
benchmark:

- `Lock:tuple` — two workers are claiming the same row. Classic
  signature of a system that doesn't use `SKIP LOCKED` (or uses it
  with overlapping candidate sets).
- `Lock:transactionid` — workers waiting on each other's
  transaction commit. Often appears with `Lock:tuple`; the second
  worker has acquired the row lock but still has to wait for the
  first transaction to clear.
- `LWLock:WALWrite`, `LWLock:WALInsert` — write amplification or
  fsync pressure from each job's commit. Adapters that batch
  completions usually have a much lower share here.
- `IO:DataFileRead` — buffer cache thrashing. If this is high during
  the clean phase, the working set has spilled out of
  `shared_buffers`.
- `Client:ClientRead` after filtering — should be near zero in our
  setup (we filter `state != 'idle'`); seeing it means a backend
  is in the middle of a query but blocked on the client side.
- `CPU:CPU` (synthetic) — the backend was running, not waiting.
  A high share here is *good* — it means the system is bottlenecked
  on its own work, not on PG resources.

## Reading the bar chart

The `wait_events_clean` plot in `index.html` shows one bar per system,
each segment being one wait-event type as a fraction of total active
samples during the clean phase. Quick interpretation guide:

- Dominantly **CPU** → adapter is bottlenecked in user-space (Rust,
  Elixir, Python overhead) or in producer→completion pipelining,
  not in PG.
- Dominantly **Lock / LWLock** → contention. Look at
  `summary.json -> systems.<sys>.phases.clean.wait_events.top` for
  the specific events.
- Dominantly **IO** → working set doesn't fit in `shared_buffers`,
  or autovacuum is reading dead tuples back in.
- A mix → typical at moderate load. The interesting reading is how
  the mix shifts between systems at the *same* offered load.

## Limitations

- 1 s sampling cadence misses sub-second contention bursts. Drop
  `--wait-event-sample-every` to e.g. 0.25 if you suspect short-lived
  spikes; the overhead is still negligible.
- We don't capture `query` text. That would let us attribute waits
  to specific queries (claim vs ack vs cleanup) but `pg_stat_activity`
  truncates at `track_activity_query_size` and the cardinality
  explodes the storage. Follow-up: see issue tagged
  `wait-events-query-attribution`.
- Single-PID samples — we don't track `xact_start`, so we can't
  derive *durations*, only sample counts. Counts are a good enough
  proxy at fixed cadence.

## Disabling

```
uv run python bench.py run --no-wait-events ...
```

Default cadence is 1 s; tune with `--wait-event-sample-every 0.5` etc.
