# river-bench adapter audit

Sub-agent (Explore) report from 2026-05-09T00:00Z.
Tooling: file:line citations against river-bench at HEAD (main.go),
River upstream at `v0.35.1` (river-bench/go.mod).

## Findings

### 1. Producer batching — documented bulk API used correctly
`main.go:206–235` (scenarioEnqueueThroughput) and `main.go:265–282`
(scenarioWorkerThroughput) use `client.InsertManyFast()`, which is the
documented bulk-insert API for River (wraps Postgres COPY). Batch size
is hardcoded to 500 in these fast-path scenarios. The long_horizon
scenario (`main.go:535`) respects `PRODUCER_BATCH_MAX` env var with a
default of 1 (single-row insert); batch logic at `main.go:670–686`
dispatches single rows via `Insert()` or multi-row via `InsertManyFast()`
based on batchCount. Bulk-insert is correctly applied per documented API.

### 2. Client config — deliberate tuning vs River defaults
`main.go:285–293`, `main.go:333–341`, `main.go:577–585` all set
`FetchCooldown: 50ms` and `FetchPollInterval: 50ms`. River's documented
defaults (per pkg.go.dev/github.com/riverqueue/river#Config) are much
longer; the 50ms values are a deliberate tightening for benchmark
fairness. `JobTimeout: -1` disables timeout (always allow jobs to
complete). MaxAttempts is not set, so River's default (`MaxAttempts:
25`) applies; this is not tuned and matches standard River semantics.
Queue config (`main.go:286–288`) sets only `MaxWorkers`, leaving all
other QueueConfig fields at their defaults.

### 3. No discarded job / DLQ handling observable
The worker functions (BenchWorker, ChaosWorker, LongHorizonWorker at
`main.go:41–42`, `main.go:58–60`, `main.go:427–444`) always return
`error(nil)` or complete successfully. There is no code path that calls
`Job.WorkerNext()` with a failure, no retry-exhaustion path, and no
discarded-job lifecycle hook. River's DLQ (state='discarded') remains
unexercised by this bench; the adapter measures throughput and latency
for jobs that never fail or exhaust retries.

### 4. Connection resilience — River.Client handles internally
`main.go:107–118` creates a single shared pgxpool with `MaxConns: 20`
(default from `MAX_CONNECTIONS` env var). The River.Client created at
`main.go:285`, `main.go:333`, `main.go:577` uses this pool and manages
its own connection lifecycle and error recovery internally (River's
documented behaviour). No explicit reconnect or chaos-handling logic is
exposed in the bench adapter; River's built-in resilience is relied upon.
The bench does *not* wrap River's client in a reconnect layer or test
explicit connection-loss scenarios at the adapter boundary.

### 5. Multi-replica isolation — no leader election; shared queue
The adapter accepts `BENCH_INSTANCE_ID` env var (`main.go:451`) and
stamps it onto metrics but does not configure River for per-replica
isolation. Every replica runs the same `river.NewClient(...)` against
the same shared database and queue (`river.QueueDefault`). River's
documented internal leader election and job-claiming semantics should
naturally distribute work across replicas, but the adapter does not
configure any explicit isolation mode (e.g., queue-per-replica,
subconsumer grouping, or affinity). Multi-replica rows in the bench
therefore measure River's native contention / coordination, not isolated
replica topology.

### 6. RescueStuckJobsAfter — only set in worker_only chaos mode
`main.go:880` sets `RescueStuckJobsAfter: time.Duration(rescueAfterSecs)
* time.Second` (defaulting to 15s from env var `RESCUE_AFTER_SECS`) only
in the `worker_only` chaos scenario. In all other scenarios
(enqueue_throughput, worker_throughput, pickup_latency, long_horizon),
RescueStuckJobsAfter is not set, so River's default (5 minutes) applies.
This means hung or stuck jobs in non-chaos scenarios will not be rescued
for 5 minutes, which is longer than the bench's typical timeout windows.

### 7. PRODUCER_BATCH_MAX default — conservative for throughput
`main.go:535` defaults `PRODUCER_BATCH_MAX` to 1 (single-row inserts).
This is intentionally conservative and will under-measure bulk-insert
throughput vs. batched production at higher rates. The long_horizon
scenario (which measures sustained throughput + latency) uses this
default unless overridden via env var, meaning the bulk-insert fast path
(`InsertManyFast` at lines 679–685) is rarely exercised unless
`PRODUCER_BATCH_MAX > 1` is explicitly set.

### 8. Soft shutdown timeout — 5 seconds, potential in-flight job loss
`main.go:817–819` (long_horizon cleanup) and lines 306–308,
363–365 (other scenarios) call `client.Stop(ctx)` with a 5-second
context timeout. Jobs in-flight at cancellation may be left uncompleted
if they cannot finish within that window. River's semantics treat
unfinished jobs as retriable, but the bench does not explicitly track
or report lost in-flight work on shutdown.

## Recommendations

1. **PRODUCER_BATCH_MAX default**: Increase default to 128 or 500 to
   exercise the documented bulk-insert path by default; 1 is too
   conservative for fair throughput comparison.
2. **RescueStuckJobsAfter**: Unify across scenarios; set to 30–60 seconds
   to match the typical bench timeout horizon, not River's 5-minute default.
3. **Discarded jobs / DLQ**: Add a deliberate failure path (e.g., 1% of
   jobs fail and exhaust retries) to measure DLQ transition and ensure
   River's failure handling is observable.
4. **Multi-replica mode**: Document whether multi-replica runs measure
   River's internal leader-election scaling or shared-queue contention;
   consider adding a per-replica-queue mode for isolated comparison.
5. **Chaos resilience**: Consider wrapping the producer/consumer to
   inject explicit connection loss and measure River's internal recovery.
