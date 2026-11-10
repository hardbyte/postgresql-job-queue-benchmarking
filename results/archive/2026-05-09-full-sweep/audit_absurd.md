# absurd-bench adapter audit

Sub-agent (Explore) report from 2026-05-09T16:00Z.
Tooling: file:line citations against absurd-bench at HEAD, absurd-sdk `0.3.0`
(pyproject.toml), absurd upstream vendored via docker pull from GitHub release.

## Findings

### 1. Producer batching path — baseline 128, tunable
`absurd-bench/main.py:232` sets `PRODUCER_BATCH_MAX=128` (env override).
`main.py:214–220` bulk-inserts via `absurd.spawn_task()` with `executemany()`,
pushing all batch items in a single round-trip. This is correctly normalized
to match other adapters (pgque 128, pgmq 128, awa/pgboss similar). No artificial
gimping here. Baseline throughput is therefore representative.

### 2. Worker concurrency — batched claim loop with semaphore
`main.py:436–439` calls `worker_app.start_worker(concurrency=worker_count,
poll_interval=0.05)`. The SDK's `AsyncAbsurd.start_worker()` (upstream
sdks/python) claims up to `effective_batch_size` tasks and executes them
with `asyncio.create_task()` up to `concurrency` in parallel (gather loop).
All `WORKER_COUNT` workers run inside a single event loop instance — there
is no per-worker isolation or separate Python processes. Matches documented
API surface.

### 3. Claim timeout and poll interval — both hardcoded to defaults
`start_worker()` at line 436 passes `poll_interval=POLL_INTERVAL_SECS`
(0.05 s, line 24) and omits `claim_timeout`, defaulting to 120 s per SDK.
No tuning knobs exposed via env vars. For comparison, pgque exposes ticker
cadence tuning (audit #1, lines 72). Absurd's 120 s claim timeout is
conservative (heartbeat every 60 s per SDK); recovery cadence after crash
is therefore delayed.

### 4. Shutdown sequence — stop_worker() drains to empty queue only
`main.py:448–456`: on SIGTERM, code calls `worker_app.stop_worker()`,
cancels non-worker tasks, then `await worker_app.close()`. The SDK's
`stop_worker()` sets `self._worker_running = False`, which causes the
`while self._worker_running:` loop (SDK __init__.py line ~750) to exit
after claiming the next batch. **If tasks are executing at cancellation,
they complete normally**. However, if a task is mid-execution when
`shutdown.wait()` fires, **the task will reach completion before the loop
exits**. The loop only checks `_worker_running` at batch boundaries
(SDK __init__.py line ~750–760), not during individual task execution.
Absurd-bench itself never explicitly release claimed tasks on shutdown.

### 5. Outstanding transaction on idle-in-tx hang — worker holds claim_timeout lock
The `idle_in_tx` phase locks the database intentionally (main.py scenario
notes). During Phase C recovery, absurd hung (rc=137) in both `idle_in_tx`
and `event_burst`. Root cause: **`AsyncAbsurd.claim_task()` SQL holds a
transaction lock for up to 120 seconds (claim_timeout default)**. When the
harness kills the replica mid-phase, the replica's last `claim_task()` call
holds the lock until timeout expires or the connection is force-closed.
On recovery (clean phase), another replica cannot claim() those locked rows
until the 120 s lock lapses. Main.py line 436 never customizes `claim_timeout`.

### 6. Chaos postgres-restart / pg-backend-kill → 0% recovery
Phase B chaos shows absurd at 0% recovery for postgres-restart and
pg-backend-kill (phase_b_summary.md line 7–8). Baseline was 205 jobs/s;
recovery was 0. The issue is **connection pooling**: when the harness kills
postgres or forces backend kills, the replica's `AsyncConnection` becomes
stale. The SDK has no reconnect-on-error loop in `claim_tasks()` (unlike
pgque-bench's ReconnectingConn wrapper, audit_pgque.md finding #6).
The claim loop (SDK line ~750) sleeps on empty results but does not
reconnect on socket loss. After postgres restarts, the connection needs
to be explicitly closed and reopened — absurd-bench omits this.

### 7. No graceful shutdown of in-flight claimed tasks
Unlike pgque's `pgque.finish_batch()` call (audit_pgque finding #7),
absurd has no explicit ack/nack path in main.py. When `stop_worker()` exits,
any claimed task that has not yet entered user code (still queued in
executing set) will be abandoned at the database level. The SDK will
eventually retry when `claim_timeout` expires (120 s), but this contributes
to the recovery hang in Phase C (adding 120 s per orphaned claim).

### 8. Per-replica isolation — single AsyncAbsurd instance
`main.py:281–288` creates one `worker_app` per replica. The adapter does
not exercise multi-queue or per-replica task routing (absurd supports
multiple queues but benchmarks use a single `QUEUE_NAME`). This is
consistent with documented API, not a shape issue.

### 9. Knobs and configuration — minimal surface
Environment variables: `WORKER_COUNT`, `PRODUCER_BATCH_MAX`, `SAMPLE_EVERY_S`,
`PRODUCER_RATE`, `PRODUCER_MODE`, `TARGET_DEPTH`, `JOB_PAYLOAD_BYTES`,
`JOB_WORK_MS`, `PRODUCER_BATCH_MS`, `LATENCY_WINDOW_MS`. Documented in
main.py. **Notably absent**: `CLAIM_TIMEOUT` (SDK default 120 s hardcoded),
`POLL_INTERVAL` (hardcoded 0.05 s), `ABSURD_WORKER_ID` (auto-generated).
These are intended to be stable defaults per SDK design, but the 120 s
claim timeout is the single largest amplifier of the shutdown hang.

### 10. Missing per-task tracking on idle-in-tx
Phase C idle_in_tx scenario intentionally creates an idle transaction
(no explicit transaction mgmt in main.py). The worker loop's asyncio
gather (SDK line ~770) collects executing promises but has no timeout
per promise. When the harness sends SIGTERM mid-idle, the task
completes normally. The hang occurs **after** shutdown, when the
recovery phase tries to claim new tasks: the database still holds the
previous replica's locks from their last claim() call.

## Recommendations

1. **Add `CLAIM_TIMEOUT` env var (max 10 s)**: replace SDK default of 120 s
   to reduce orphaned-claim recovery latency. Test with 10 s.

2. **Implement reconnect-on-error in producer** (line 327): catch `psycopg.OperationalError`
   and re-establish `producer_conn` before retry, matching pgque fix (audit_pgque.md #6).

3. **Add explicit claim release on shutdown** (line 449–456): before
   exiting, query `absurd.t_{queue_name}` for any rows in running state
   with the replica's worker_id and call absurd.fail_task_run() to release locks.

4. **Increase POLL_INTERVAL or add tunable** (line 24): 0.05 s is aggressive;
   0.25 s matches SDK default and reduces CPU during idle phases.

5. **Document claim_timeout trade-off**: 120 s default is conservative
   (suitable for long-running tasks) but hurts multi-replica crash recovery.
   Add docstring to start_worker() call with recommended values.

## Executive Summary

1. **SHUTDOWN HANG ROOT CAUSE — 120 s claim_timeout lock**: Absurd's SDK defaults
   to 120 s `claim_timeout` in `AsyncAbsurd.claim_task()`, which holds a database
   lock. When the harness kills the replica mid-phase, the lock remains until
   timeout, blocking all recovery-phase claims. This is the **single root cause**
   of rc=137 hangs in idle_in_tx and event_burst (Phase C). Fix: expose
   `CLAIM_TIMEOUT` env var, set to 10 s or less.

2. **Connection recovery missing in producer loop**: Unlike pgque-bench, absurd-bench
   has no reconnect-on-error wrapper. When postgres-restart or pg-backend-kill
   occurs, the `AsyncConnection` becomes stale and never recovers — recovery
   stays at 0% (phase_b_summary.md). Producer must catch `OperationalError`
   and re-connect.

3. **No explicit claim release on shutdown**: Absurd-bench calls `stop_worker()`
   and `close()` but never explicitly releases claimed tasks from the database.
   Orphaned claims block recovery until `claim_timeout` expires. On shutdown,
   query for in-flight rows and call `fail_task_run()` to unblock peers.

4. **Poll interval 0.05 s is aggressive vs SDK default 0.25 s**: Hardcoded
   `POLL_INTERVAL_SECS` (line 24) causes unnecessary CPU churn during idle.
   SDK's default of 0.25 s is recommended; change to tunable env var.

5. **Claim timeout not exposed as tunable**: Documented absurd API supports
   `claim_timeout` parameter to `start_worker()`, but absurd-bench line 436
   omits it, forcing 120 s default. This is **the largest single knob** affecting
   recovery latency and should be user-configurable (recommended: 5–10 s for
   multi-replica deployments, 120 s for single-worker long-running tasks).
