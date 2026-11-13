# pgboss-bench adapter audit

Sub-agent (Explore) report from 2026-05-09T00:00Z.
Tooling: file:line citations against pgboss-bench at HEAD of branch
`bench/2026-05-09-full-sweep`, pg-boss version 12.18.2 (package.json line 12).

## Findings

### 1. Producer uses batch insert API
`pgboss-bench/main.js:268` calls `boss.insert(QUEUE_NAME, jobs)` with an array of job objects. This is pg-boss's native batch insert path (bulk SQL) rather than individual enqueue calls. The batch size is tuned via `PRODUCER_BATCH_MAX` (line 140, default 128) and rate-limited by `producerBatchMs` (line 139, default 10 ms). Insertion latency is measured per-job (line 270) and tracked in `producerLatencies`. No alternative path (e.g., sendMany) is exercised.

### 2. Worker concurrency and batch consumption
`boss.work()` call at line 205-231 explicitly configures: `pollingIntervalSeconds: 0.5`, `localConcurrency: workerCount` (line 209), and `batchSize: subscriberBatchSize` (line 210). The `subscriberBatchSize` is computed dynamically (lines 141-144) as `max(1, min(64, ceil((producerRate * 1.25) / max(workerCount * 2, 1))))`, which adapts batch size to target rate and worker count. Each batch of jobs is processed in a tight loop (lines 214-229); all latencies are measured before and after the simulated `workMs` delay (line 221).

### 3. Retry and DLQ unexercised
The work handler at lines 212-231 processes every job synchronously and completes successfully. There is no call to `job.failed()`, `job.setFailed()`, or any explicit error throwing that would trigger pg-boss's retry / DLQ archive path. All jobs that make it to the handler exit cleanly. This means job failure and retry behaviour are unobserved.

### 4. No connection recovery on database restart — primary chaos failure
`pgboss-bench/main.js:147-152` creates a `PgBoss` instance with `noSupervisor`, `noScheduling`, and `noMonitoring` flags. The instance has a minimal error handler at lines 154-156 that only logs. **Critically, there is no connection recovery logic in the producer (lines 234-285), depth monitor (lines 287-302), or sampler (lines 304-358) tasks.** When PostgreSQL is stopped during `chaos_postgres_restart` or backends are killed during `chaos_pg_backend_kill`, pg-pool's internal connections die with FATAL 57P01 / 57P03 errors (chaos_postgres_restart_pgboss.log lines 1190-1193). The producer and depth tasks do not catch or reconnect; they propagate the unhandled exception up through the task promise, which crashes the process (rc=1, line 1258 of run log). Unlike pgque's documented reconnection fix (audit_pgque.md finding 6), pgboss-bench has no `try-catch-reconnect` wrapper. The adapter cannot survive a Postgres restart.

### 5. Per-replica isolation via independent PgBoss instances
Each replica (via `BENCH_INSTANCE_ID` line 27) starts its own `PgBoss` instance with the same queue name. pg-boss's internal supervisor handles per-instance job claiming (when supervisor is enabled; here it is disabled via `noSupervisor: true` line 149, to reduce overhead). Since supervisor is disabled, claiming is still managed by the pool-level concurrency and pg-boss's built-in fetch logic (configured via `localConcurrency`). Multi-replica runs do correctly isolate: each replica's workId and task loops are independent (line 205, workId is replica-local). Contention is minimized via pg-boss's partitioned queue (line 161, `partition: true`).

### 6. Disabled monitoring and scheduling for minimal overhead
Lines 149-151 explicitly disable `noSupervisor`, `noScheduling`, and `noMonitoring`. This is a documented optimization pattern (reducing polling and metadata churn) but means built-in auto-recovery features are off. The adapter must handle reconnection and graceful shutdown manually — which it does not (finding 4). Shutdown at line 363 is graceful but offers no pre-shutdown flush of in-flight jobs; any job held by a worker at task cancellation (line 362 `Promise.allSettled`) will be left unacked and will retry only after pg-boss's default reap timeout (~10–30 s, depending on supervision/monitoring).

## Recommendations
1. **Critical**: Wrap producer, depth, and sampler task loops in `try-catch-reconnect` logic. On `OperationalError` / connection loss, close the pool and retry connection via `boss.start()`. This is the primary blocker for chaos test survival.
2. Restore `noSupervisor: false` or implement explicit connection health checks and reconnect attempts, so the adapter recovers from `chaos_postgres_restart` and `chaos_pg_backend_kill` events.
3. Exercise the DLQ path by simulating job failure (throw in handler or use `job.failed()`) so retry and dead-letter behaviour are observable.
4. Document the timeout and reap semantics for in-flight jobs under graceful shutdown.

## 5-Bullet Summary (Chaos Failure Mode Prioritized)
- **[CRITICAL FAILURE MODE]** No connection recovery on Postgres restart: producer/depth/sampler tasks fail with unhandled `FATAL 57P0x` errors when `chaos_postgres_restart` or `chaos_pg_backend_kill` stops the database. Process exits rc=1 with no reconnect attempt, unlike pgque's documented fix. Direct cause of rc=1 exit in Phase B chaos tests.
- Producer uses batch insert (`boss.insert()` line 268) with tuned batch size (default 128, configurable via `PRODUCER_BATCH_MAX`) and fixed 10 ms inter-batch delay, meeting the adapter contract.
- Worker concurrency hardcoded at instantiation (`localConcurrency: workerCount` line 209, default 32) with adaptive batch size (lines 141-144) tuned to producer rate and worker count; polling interval fixed at 0.5 s.
- Retry and DLQ paths unexercised: handler never calls `job.failed()` or throws; all jobs complete synchronously (lines 212-231). Job failure semantics are unobserved.
- Multi-replica isolation via independent PgBoss instances with partitioned queue (`partition: true` line 161); supervisor disabled (`noSupervisor: true` line 149) for minimal overhead, but this also disables built-in recovery and auto-claiming features.
