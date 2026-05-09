# pgmq-bench adapter audit

Sub-agent (Explore) report from 2026-05-09T00:00Z.
Tooling: file:line citations against pgmq-bench at HEAD (main.py),
pgmq upstream at `v1.11.1` (pgmq-bench/main.py:17–18, container image
`ghcr.io/pgmq/pg18-pgmq:v1.11.1`).

## Findings

### 1. Producer batching — using documented bulk API correctly
`pgmq-bench/main.py:244–248` calls `pgmq.send_batch(%s, %s)` with a
batch of messages. Batch size is configured via `PRODUCER_BATCH_MAX` env
var (default 128, `main.py:152`). The bulk-insert path is exercised by
default, matching pgque and awa. Producer latency is measured per-message
within the batch (line 253: `per_msg_ms = elapsed_ms / max(len(batch), 1)`),
correctly amortizing batch overhead.

### 2. Visibility timeout — using pgmq.read(), not read_with_poll()
`main.py:267–268` calls `pgmq.read(queue_name => %s, vt => %s, qty => %s)`
with visibility_timeout_s from `VISIBILITY_TIMEOUT_S` env var (default 30s,
line 154). The pgmq extension also exposes `pgmq.read_with_poll()`, which
blocks up to max_poll_duration_ms waiting for at least one message; this
path is unexercised. The adapter polls with fixed `POLL_INTERVAL_MS`
(default 50ms, `line 153`) in the consumer loop's empty-result branch
(line 272), meaning long poll-blocks are not used.

### 3. Archive table behaviour — single archive, no partitioning
`main.py:173–174` declares event tables `pgmq.q_<queue_name>` and
`pgmq.a_<queue_name>`. Archive is handled via `pgmq.archive(%s, %s)`
(line 289), which moves messages from queue to archive table. pgmq's
archive table is created by `pgmq.create()` (line 118) with no explicit
partitioning configuration. No calls to `pgmq.purge_archive()` or
retention-lifecycle functions; messages accumulate in the archive table
for the duration of the run, potentially creating a growing hot table.

### 4. Anti-scaling pattern — consumer_batch_size formula inverts concurrency
`main.py:155–158` computes `consumer_batch_size =
max(1, min(64, round((producer_rate * 1.25) / max(worker_count * (1000 /
max(poll_interval_ms, 1)), 1))))`. With producer_rate=800, poll_interval_ms=50:
- W=4: batch_size=12 (48 items/poll cycle)
- W=16: batch_size=3 (48 items/poll cycle)
- W=64: batch_size=1 (64 items/poll cycle, but each reader does tiny
  SELECT...FOR UPDATE)
- W=128: batch_size=1 (128 concurrent single-row claims)
- W=256: batch_size=1 (256 concurrent single-row claims)

The formula tries to normalize throughput but at W≥64, pgmq.read() likely
uses row-level locking (FOR UPDATE) to claim messages and update vt
(visibility timeout). With 64+ concurrent workers each claiming 1 row at a
time, lock contention explodes: Phase A completion rates show W=16 peak at
11.3k jobs/s, W=64 drops to 10.2k, W=128 to 6.2k, W=256 to 3.2k. The
queue_depth_median also collapses (W=16: 3,872 → W=64: 404 → W=256: 296),
indicating readers consume faster than producers but at lower throughput
due to lock serialization.

### 5. Active_readers chaos — 39% completion due to snapshot contention
`pgmq-bench/main.py` uses autocommit=true (line 108), so each read/archive
call is its own transaction. Phase C active_readers stress test launches 4
REPEATABLE READ scanner threads via `bench_harness/hooks.py:_reader_loop()`
(lines 29–40), which execute `SELECT count(*) FROM pgmq.q_long_horizon_bench`
in a non-autocommit connection with isolation level REPEATABLE READ. These
long-lived snapshots reduce MVCC visibility and increase snapshot overhead
for pgmq.read() visibility checks (the `WHERE vt <= now()` predicate).
Completion rate drops to 39% (Phase C: 310 jobs/s vs clean 800 jobs/s),
indicating severe reader-writer contention. The root cause is snapshot
isolation thrashing: REPEATABLE READ scanners hold old snapshots that
force pgmq's visibility scans to age and create tuple visibility overhead.

### 6. Shutdown hang — rc=137 timeout in idle_in_tx and event_burst recovery
Phase C chaos_modes idle_in_tx and event_burst terminate with rc=137
(timeout), meaning the adapter process did not exit within the harness
timeout. `main.py:382–387` (shutdown sequence) cancels tasks with a
bare `asyncio.gather(*tasks, return_exceptions=True)` and closes
producer_conn and depth_conn. If a consumer task is mid-archive at
cancellation, the connection close is immediate; if a message was claimed
(vt updated) but not archived, it is left in the queue with an old vt,
waiting for timeout. Recovery phase tries to re-consume but the old VT
messages are not yet available (still in timeout), causing stalls. The
hang suggests a cascading failure: idle transactions from the chaos phase
block pgmq.read() lock acquisition in recovery.

### 7. Chaos resilience — 0% recovery in postgres_restart / pg_backend_kill
Phase B postgres_restart and pg_backend_kill scenarios show 0% completion
recovery (Phase B table: pgmq baseline ~600 jobs/s, recovery 0). The
consumer loop (line 264–303) does not reconnect on connection loss. When
postgres restarts or backends are killed, the connection handle becomes
stale; subsequent pgmq.read() calls fail silently or raise exceptions that
are not caught. Unlike pgque (which has explicit reconnect via
ReconnectingConn, per the pgque audit), pgmq-bench has no reconnect logic.
The consumer loop will log or swallow the error and continue in a broken
state, never recovering. No explicit exception handling for
`psycopg.OperationalError` or `ConnectionDoesNotExist`.

### 8. Undocumented knobs and missing tuning
pgmq.create() is called with no configuration arguments (line 118). The
extension likely has tunable parameters (retention, vacuum policy,
lock_timeout_ms, batch_queue_depth) that are not exposed in the adapter.
No environment variables control pgmq-specific settings (unlike pgque's
ticker configuration or pgboss's pool size). The adapter measures pgmq's
defaults end-to-end, but does not exercise any tuning surface that might
improve contention or throughput.

## Summary Table

| Finding | Impact | Root Cause | Severity |
|---------|--------|-----------|----------|
| Consumer batch size bottleneck at W≥64 | 11.3k→3.2k jobs/s as W scales | FOR UPDATE row locking with 64+ concurrent single-row claims | **High** |
| active_readers snapshot contention | 39% completion (310 vs 800 jobs/s) | REPEATABLE READ snapshots block MVCC visibility for pgmq's vt scans | **High** |
| No connection recovery on chaos | 0% recovery in postgres_restart / pg_backend_kill | Consumer loop does not reconnect after socket loss | **High** |
| Shutdown hang rc=137 | Timeouts in idle_in_tx and event_burst recovery | Leftover claimed messages with stale VT block recovery; no explicit graceful closure | **Medium** |
| read_with_poll() not exercised | Adapter polls manually instead of using pgmq's blocking API | Design choice, not a defect, but leaves polling overhead in consumers | **Low** |
| No archive lifecycle management | Archive table grows unbounded | No pgmq.purge_archive() or retention config; archive becomes hot table | **Low** |
| No pgmq tuning surface exposed | Baseline performance only; unknown headroom from config | pgmq parameters not wired to env vars | **Low** |

## Recommendations

1. **Investigate FOR UPDATE contention at W≥64**: Profile pgmq.read() at high worker counts (64+). Verify that row-level locks on vt updates are the bottleneck. Consider whether pgmq supports a non-locking polling mode or bulk-claim variant.

2. **Add connection recovery**: Wrap consumer/producer/depth connections in a small reconnect helper (cf. pgque-bench's ReconnectingConn) that catches `psycopg.OperationalError` and `ConnectionDoesNotExist`, closes the stale connection, and opens a new one. Test recovery in postgres_restart and pg_backend_kill phases.

3. **Tune consumer_batch_size formula**: At W≥64, the formula produces single-row batches. Consider increasing `CONSUMER_BATCH_SIZE` for high-concurrency cases to reduce per-message overhead. A fixed minimum (e.g., 16) might improve throughput.

4. **Use read_with_poll() or add explicit wait**: Replace manual `asyncio.sleep(poll_interval_ms)` with pgmq's blocking read API to reduce busy-polling overhead and latency variance.

5. **Graceful shutdown of claimed messages**: Before exiting, explicitly archive any in-flight message IDs held by consumer tasks, or extend the shutdown timeout to allow in-flight work to complete.

6. **Add snapshot isolation tuning**: For active_readers chaos, consider running a VACUUM or reducing autovacuum_naptime to improve snapshot overhead. Or isolate the chaos readers to a separate connection with different isolation level if permitted.

7. **Archive lifecycle**: Call `pgmq.purge_archive()` periodically (e.g., at run end) to prevent archive table bloat. This is not a bug but a best practice for production pgmq usage.


---

## 5-Bullet Summary

1. **Anti-scaling bottleneck (W≥64)**: consumer_batch_size formula drops to single-row per worker at W=64+. Each of 64+ workers executes a separate `pgmq.read(qty=1)`, which uses row-level FOR UPDATE locking to claim and update vt. Concurrent lock contention on the same hot rows causes completion rate to collapse from 11.3k (W=16) → 3.2k (W=256) jobs/s. Root cause: pgmq's row locking model does not scale with >32 concurrent claimers; the formula inadvertently maximizes lock contention.

2. **active_readers cliff (39% completion)**: REPEATABLE READ scanner threads create long-lived snapshots that increase MVCC overhead for pgmq's visibility checks (`WHERE vt <= now()`). Throughput crashes from 800 to 310 jobs/s (39% recovery ratio). pgmq is sensitive to snapshot isolation thrashing; concurrent readers in READ COMMITTED or READ UNCOMMITTED modes (not tested) might recover better.

3. **Chaos resilience failure (0% recovery)**: Consumer loop does not reconnect after connection loss in postgres_restart and pg_backend_kill scenarios. Unlike pgque-bench (which implements ReconnectingConn), pgmq-bench has no exception handling for `psycopg.OperationalError`. Stale connections silently fail; recovery phase never restarts consumption.

4. **Shutdown hang (rc=137)**: idle_in_tx and event_burst recovery phases timeout. Likely cause: claimed but unarchived messages left in queue with stale vt values block recovery; cascading idle transactions prevent new pgmq.read() operations from acquiring locks. Graceful shutdown logic needed to flush in-flight messages before exit.

5. **Unexercised API surface**: `pgmq.read_with_poll()` (blocking wait for at least 1 message) is documented but not used; adapter manually polls with 50ms sleep instead. No tuning exposed for pgmq's internal parameters (retention, lock timeouts, batch queue depth). Baseline measurement only; unknown headroom from knob configuration.
