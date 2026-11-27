# awa 0.6.19 rerun of the November comparison

awa ran again through every scenario of the [November sweep](../2026-11-12-full-sweep/SUMMARY.md), on awa 0.6.19. Same host (AMD Ryzen 9 5900X, 24 threads, 61 GB), PostgreSQL 18.6 capped at 4 CPUs, the same settings, and library defaults. The run took place on 27 November 2026, 08:57–13:19 UTC, with harness commit 022487e7.

The website layers these results over the sweep. The other seven systems keep their 11–16 November results. awa 0.7 (unreleased) stays a separate variant from [2026-11-16-operator-awa07](../2026-11-16-operator-awa07). Export with:

```bash
uv run python scripts/export_site_results.py results/2026-11-12-full-sweep results/2026-11-13-rerun \
  results/2026-11-13-followup results/2026-11-16-operator results/2026-11-27-awa0619 \
  results/2026-11-16-operator-awa07 \
  --variant "results/2026-11-16-operator-awa07:awa=awa-v07:Awa 0.7 (unreleased)" \
  --cells-dir results/2026-11-12-full-sweep/cells
```

Each cell ran once.

| Measure | 0.6.17+git.1a53fd9e | 0.6.19 |
|---|---:|---:|
| jobs/s, 4 workers | 488 | 480 |
| jobs/s, 16 workers | 1,717 | 1,817 |
| jobs/s, 64 workers | 5,689 | 6,348 |
| jobs/s, 128 workers | 8,573 | 10,682 |
| jobs/s, tuned, 1 queue | 8,550 | 10,614 |
| jobs/s, tuned, 4 queues | 10,762 | 11,296 |
| pickup p99 at 800 jobs/s, ms | 19.0 | 52.0 |
| idle transactions/s | 60.1 | 26.2 |
| idle Postgres private memory, MB | 276 | 250 |
| idle WAL, B/s | 3,909 | 1,484 |
| backlog cleared after a Postgres restart, s | 10.0 | 15.0 |
| neighbour p99 with the queue at 4×, ms | 6.5 | 3.2 |
| dead catalog tuples, stalled slot | 11,374 | 11,492 |
| scheduled herd lateness p99, ms | 11,975 | 11,548 |
| immediate pickup p99 during the herd, ms | 18.5 | 49.0 |
| pickup p99 in the retry storm, ms | 24.0 | 52.0 |
| pickup p99, 1 queue, ms | 19.0 | 49.0 |
| pickup p99, 200 queues, ms | 85.0 | 63.5 |
| idle transactions/s, 200 queues | 667 | 125 |
| idle Postgres cores, 200 queues | 0.24 | 0.10 |
| database after 64 KiB payloads, MB | 22.2 | 22.2 |
| database after 64 KiB arguments, MB | 1,218 | 994 |
| database after 1 MiB arguments, MB | 823 | 524 |
| database after an 8 GiB drain, MB | 21.3 | 21.3 |
| WAL with 40 s jobs, B/s | 27,929 | 24,905 |
| backlog drain, jobs/s | 5,154 | 6,093 |
| get_by_id p99, ms | 103 | 233 |
| find by unique field p50, ms | 30,001 | 30,001 |
| kind/state page p50, ms | 1,413 | 1,182 |
| count by state p99, ms | 149 | 266 |
| first start after resume, ms | 28.0 | 35.0 |
| jobs started while paused | 0 | 0 |
| 1M abandoned jobs done again, s | 1,231 | 650 |
| rescue-and-rerun rate, jobs/s | 919 | 1,975 |
| missing jobs | 0 | 0 |

## Reading the changes

- **Throughput:** up from 64 workers.
- **Idle cost:** idle transactions and WAL are down by more than half, and 200 idle queues cost 125 transactions/s rather than 667.
- **Pickup latency at steady load:** about 50 ms at p99, against 19–24 ms before. 0.6.19 coalesces its wake-up notifications into at most one per channel and connection pool every 50 ms. That trades latency at low and steady rates for fewer transactions.
- **Mass abandonment, 1,231 s → 650 s:** much of this is the harness, not awa. The earlier figure included the harness's own long state query, which held locks that claim prune needed. The harness's state queries now run under a statement timeout.
- **`get_by_id` p99:** varies widely between runs on 0.6.19. This run measured 233 ms; two release runs on the same host measured 27 and 112 ms.

## Excluded run

The first `neighbour_shared` run is listed in `run_index.excluded.tsv`. It caught a stall of about 5 seconds across the whole server during the baseline phase, 73–83 s into the run, with the queue idle.

- The neighbour's throughput fell from 310 to 44 transactions/s, then caught up at 564.
- Postgres CPU and WAL barely moved for those 5 seconds.
- The neighbour's service time stayed normal, at 2.65 ms p99.
- awa's pickup p99 stayed level.

That stall set the phase's p99 at 4,463 ms. The cell ran again straight after the sweep, and that run is the one published.
