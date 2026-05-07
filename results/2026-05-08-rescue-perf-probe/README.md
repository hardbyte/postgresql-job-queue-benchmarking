# Rescue-ON / rescue-OFF perf probe

Side-by-side `pg_stat_statements` snapshots of awa-bench at 1×256
workers, depth-target=4000, 120 s clean phase. Two cells, identical
except `LEASE_DEADLINE_MS`:

- `rescue_off_w256` — `LEASE_DEADLINE_MS=0` (deadline rescue disabled)
- `rescue_on_w256` — `LEASE_DEADLINE_MS` unset, awa picks the library default

The probe needs `pg_stat_statements` in `shared_preload_libraries`,
which `postgres.conf` doesn't include. To reproduce, append the
following to `postgres.conf` before running:

```
shared_preload_libraries = 'pg_stat_statements'
pg_stat_statements.track = 'all'
pg_stat_statements.max = 10000
```

Then `docker compose down -v && docker compose up -d --wait` to pick
up the conf change, and run `scripts/probe.sh` from the repo root.
The script uses `KEEP_DB=1` to keep Postgres alive after each bench
cell so the snapshot can be captured before teardown.

## Headline

| | rescue OFF | rescue ON | Δ |
|---|--:|--:|--:|
| `completion_rate` (jobs/s) | 5,419 | 3,649 | **−33 %** |
| offered (depth-target burst) | 3,905 | 2,752 | −30 % |
| `end_to_end_p99_ms` | 100 | 151 | +51 % |

## Where the time goes

`snapshots/rescue_*_w256.tsv` are top-50-by-total_exec_time
extracts from `pg_stat_statements`, scoped to `awa_bench` queries
only. Top hot-path mean execution times:

| query | OFF mean ms | ON mean ms | Δ |
|---|--:|--:|--:|
| `UPDATE awa.queue_enqueue_heads SET next_seq = ...` | 14.8 | 18.0 | +22 % |
| `INSERT INTO awa.queue_enqueue_heads ON CONFLICT ...` | 5.6 | 7.3 | +30 % |
| `SELECT ... FROM awa.claim_ready_runtime(...)` | 1.28 | 1.60 | +25 % |
| `INSERT INTO awa.queue_lanes ON CONFLICT ...` | 2.9 | 3.3 | +14 % |
| `INSERT INTO awa.queue_claim_heads ON CONFLICT ...` | 2.8 | 3.3 | +18 % |
| `UPDATE awa.queue_lanes ... deltas ...` | 2.2 | 2.5 | +13 % |
| `UPDATE awa.queue_lanes SET available_count = ...` | 1.7 | 2.0 | +18 % |

No new query appears in the top-50 with rescue ON. Every hot-path
query gets 13–30 % slower uniformly. The data points at structural
contention (working-set growth on `awa.lease_claims`, or shared-lock
contention from a background scanner) rather than additive
per-claim overhead.

Filed for upstream investigation as
[hardbyte/awa#246](https://github.com/hardbyte/awa/issues/246).
