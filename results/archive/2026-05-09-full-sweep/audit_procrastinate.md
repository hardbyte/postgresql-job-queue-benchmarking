# procrastinate-bench adapter audit

Sub-agent (Explore) report from 2026-05-09T20:00Z.
Tooling: file:line citations against procrastinate-bench at HEAD,
procrastinate 3.8.1 (pyproject.toml line 6).

## Findings

### 1. Producer bulk-insert API is exercised correctly
`main.py:134-136` calls `deferrer.batch_defer_async()` documented in
procrastinate's `tasks.py:143`. Long-horizon scenario (`main.py:413-422`) 
conditionally batches: single-insert on `batch_count==1`, else multi-row 
bulk via `batch_defer_async(*kwargs_list)`. Batch parameters configurable via
`PRODUCER_BATCH_MAX` (default 1, `main.py:285`) and `PRODUCER_BATCH_MS` 
(default 10 ms, `main.py:286`). No artificial throughput handicap detected.

### 2. Worker pool and app shape are correct
`main.py:56-58` creates single `procrastinate.App` with 
`PsycopgConnector(conninfo=...)`, opened correctly via `async with 
app.open_async()` (`main.py:617`). `build_worker()` (`main.py:148-160`)
calls `app._worker(queues=[queue], concurrency=worker_count, ...)`. Worker 
is standard documented pattern; concurrency bound to `worker_count` parameter.
No `with_options()` / task-level override complexity.

### 3. Retry, DLQ, and failure handling are not exercised
Bench tasks (`bench_job`, `chaos_job`, `long_horizon_task`) are sync 
async functions that never raise exceptions (`main.py:62-63`, `66-68`,
`325-339`). No `@app.task(retry=...)` (documented in procrastinate's
tasks.py as `retry: RetryValue | None`), no DLQ codepath, no handler for 
`job.status == 'failed'` or `'aborted'`. Bench therefore exercises zero 
retry semantics and cannot measure retry latency or DLQ throughput. 
`delete_jobs="never"` (`main.py:156`, `349`) intentionally preserves jobs 
for auditing, matching pgque-bench pattern.

### 4. Chaos resilience and DB connection recovery rely on psycopg internals
Producer/depth_poller/sampler exception handlers (`main.py:425-429`,
`452`, `440-456`) catch broad `Exception` and sleep/continue. Procrastinate's 
`PsycopgConnector` (psycopg_connector.py) uses an internally-managed 
`AsyncConnectionPool` (created by `.open_async()`, line 341). On socket loss,
the pool's automatic reconnect handles recovery — the benchmark does NOT
wrap connections in a custom `ReconnectingConn` helper (unlike pgque-bench's
finding #6). Implicit reliance on psycopg pool semantics; pgque-bench's fix 
(explicit reconnect on `OperationalError`) is not replicated here. Works 
because psycopg pool reconnects transparently, but less visible in adapter code.

### 5. LISTEN/NOTIFY channels are correctly named per queue
Worker created with `listen_notify=True` (`main.py:155`, `348`). 
Procrastinate's manager.py auto-derives channels as `"procrastinate_queue_v1#" 
+ queue_name` per-queue. Benchmark uses isolated queue names 
(`portable_default`, `chaos`, `procrastinate_*_bench`) — no cross-queue 
contention. Channels set up correctly; no undocumented override.

### 6. Per-replica isolation and observer pattern correctly implemented
Long-horizon scenario uses `BENCH_INSTANCE_ID` (`main.py:531`, default 0).
Only instance 0 emits observer metrics (queue_depth, producer_target_rate)
via `_observer_enabled()` (`main.py:536-542`). Non-observer replicas skip 
depth_poller sleep loop (`main.py:433-439`). Multi-replica runs properly 
isolate polling work and avoid de-duplication burden downstream. Matches 
awa-bench pattern.

### 7. Documented config knobs used, but some worker defaults unexposed
Worker built with explicit parameters: `fetch_job_polling_interval=0.05`,
`abort_job_polling_interval=0.05`, `update_heartbeat_interval=5`, 
`stalled_worker_timeout=15` (`main.py:153-159`, `346-352`). These map to 
documented `WorkerOptions` (app.py). However, Procrastinate's default 
`worker_defaults` dict in App (app.py:__init__) is not logged or 
exposed to environment override — benchmark hardcodes values. 
Upside: explicit and reproducible. Downside: no knob to tune via env var 
(unlike producer_rate / PRODUCER_RATE).

## Recommendations

1. **Add a failure-injection scenario** to exercise retry semantics: 
   emit jobs that raise transient exceptions, tune `@app.task(retry=N)`, 
   measure job success rate and latency.
2. **Document or expose `update_heartbeat_interval` / `stalled_worker_timeout`** 
   as env vars for chaos resilience testing (e.g., HEARTBEAT_INTERVAL_S, 
   STALLED_TIMEOUT_S).
3. **Benchmark `delete_jobs="always"` vs "never"** path to measure cleanup 
   overhead (one scenario with auto-delete on completion).
4. **Test connection-pool eviction** by setting lower pool min/max size 
   via `PsycopgConnector(min_size=2, max_size=5, ...)` to stress reconnect 
   codepath more visibly.
5. **Add DLQ / nack codepath** (job failure → manual nack) to show failure 
   handling impact on throughput, not just success path.
