### Phase D — DLQ ingest

| cell | system | enqueue_rate | completion_rate | retryable_failure_rate |
|---|---|---:|---:|---:|
| `dlq_terminal_awa` | awa | 1,414 | 0 | 1,442 |
| `dlq_retry_smoke_awa` | awa | 1,790 | 1,838 | 910 |
| `dlq_terminal_pgque` | pgque | 801 | 381 | — |

### DLQ-specific metric keys present in each cell

`dlq_terminal_awa`:
  autovacuum_count@awa.attempt_state: 0.0
  autovacuum_count@awa.dlq_entries: 3.0
  last_autovacuum_age_s@awa.dlq_entries: 25.2199015
  n_dead_tup@awa.attempt_state: 0.0
  n_dead_tup@awa.dlq_entries: 0.0
  n_live_tup@awa.attempt_state: 0.0
  n_live_tup@awa.dlq_entries: 172741.5
  pgstattuple_dead_pct@awa.attempt_state: 0.0
  pgstattuple_dead_pct@awa.dlq_entries: 0.0
  pgstattuple_free_pct@awa.attempt_state: 0.0
  pgstattuple_free_pct@awa.dlq_entries: 5.05
  retryable_depth: 0.0

`dlq_retry_smoke_awa`:
  autovacuum_count@awa.attempt_state: 0.0
  autovacuum_count@awa.dlq_entries: 0.0
  n_dead_tup@awa.attempt_state: 18.0
  n_dead_tup@awa.dlq_entries: 0.0
  n_live_tup@awa.attempt_state: 8.0
  n_live_tup@awa.dlq_entries: 0.0
  pgstattuple_dead_pct@awa.attempt_state: 14.06
  pgstattuple_dead_pct@awa.dlq_entries: 0.0
  pgstattuple_free_pct@awa.attempt_state: 78.12
  pgstattuple_free_pct@awa.dlq_entries: 0.0
  retryable_depth: 2144.5
  retryable_failure_rate: 909.5779476228552

`dlq_terminal_pgque`:
  autovacuum_count@pgque.retry_queue: 0.0
  n_dead_tup@pgque.retry_queue: 0.0
  n_live_tup@pgque.retry_queue: 0.0
  pgstattuple_dead_pct@pgque.retry_queue: 0.0
  pgstattuple_free_pct@pgque.retry_queue: 0.0
  table_size_mb@pgque.retry_queue: 0.0
  total_relation_size_mb@pgque.retry_queue: 0.0234375

