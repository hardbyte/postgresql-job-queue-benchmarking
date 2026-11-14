# November 2026 job-queue comparison

Eight Postgres-backed queues ran the same scenarios on one host (AMD Ryzen 9 5900X, 24 threads, 61 GB). Each system had a fresh PostgreSQL 18.6 cluster, capped at 4 CPUs, with data on NVMe. Runs took place between 11 and 13 November 2026. The website renders these results with charts: `website/data/results.json`, built by `scripts/export_site_results.py`.

## Runs

| Results root | Dates (UTC) | Harness | What it holds |
|---|---|---|---|
| `2026-11-12-full-sweep` | 11 Nov 21:39 – 13 Nov 04:11 | `62f22cd` | Every scenario for every system. Each adapter used its default connection pool. |
| `2026-11-13-rerun` | 13 Nov 04:14 – 08:50 | `1964ee5` | Every Oban cell, after the fix to the adapter's latency window. Its unplanned pgque and pgmq large_args cells and a River abandon cell that collided with the follow-up are in `run_index.excluded.tsv`. |
| `2026-11-13-followup` | 13 Nov 08:50 – 22:14 | `50b978a` | Saturation, tuned saturation, the Postgres restart and many queues for every system. Awa (at `1a53fd9e`) and Absurd for every other scenario. River's abandon cell. Every system had a pool of workers + 8 connections per process. |

Where a later root has a system's cell, it replaces the earlier result. Only saturation, the Postgres restart and many queues compare every system under the same pool rule. In the other scenarios:
- Awa and Absurd come from the follow-up.
- Oban comes from the rerun.
- pg-boss, PGMQ, PgQue, Procrastinate and River come from the full sweep, with each adapter's default pool. That was 4 connections for Procrastinate and 10 for pg-boss.

`results/2026-11-14-absurd-ab-*` holds an A/B of the old and new absurd adapters on ref800 and the scheduled herd. Jobs/s at ref800 were 208 and 230 with the old adapter against 234 and 230 with the new. The new depth probe made absurd no slower and used about 0.15 fewer Postgres cores.

## Findings

- **Throughput:** each system's best measured configuration.

  | System | Jobs/s | Configuration | Postgres cores |
  |---|---:|---|---:|
  | PgQue | 47,282 | tuned, 1 queue | 1.1 |
  | PGMQ | 12,013 | 16 workers | 1.7 |
  | River | 10,905 | tuned, 4 queues | 2.2 |
  | Awa | 10,762 | tuned, 4 queues | 4.0 |
  | Oban | 5,673 | tuned | 3.8 |
  | pg-boss | 3,495 | tuned, Postgres-bound | 3.9 |
  | Absurd | 1,625 | tuned, 4 queues | 4.0 |
  | Procrastinate | 1,472 | tuned | 2.2 |

  - PgQue's figure is a floor: its single producer was the limit.
  - PGMQ slows beyond about 16 readers, because each read scans past the in-flight rows.
- **Pickup at 800 jobs/s:** Awa, Oban and PGMQ stay within 20 ms at p99. River's adapter cooldown caps it at 640 jobs/s with 32 workers. Procrastinate and Absurd also fall behind.
- **Postgres restart:** all eight systems recover. Absurd and Procrastinate need their worker restarted, as a supervisor would.
- **Logical replication:** Awa at `1a53fd9e` has no tables without a replica identity, down from 19 on 0.6.17. Its catalog churn under a stalled slot fell from 19,024 to 11,374 dead tuples, still the most of any system.
- **Storage:** only Awa gives disk space back after 64 KiB payloads (22 MB against about 1.2 GB) and after an 8 GiB backlog of 1 MiB jobs (21 MB against 7–16 GB).
- **A million abandoned claims:**

  | System | Done again in |
  |---|---|
  | PgQue | 16 s (batch redelivery) |
  | PGMQ | 5.9 min |
  | pg-boss | 14 min |
  | Awa | 20.5 min |
  | Absurd | 28.5 min |
  | Procrastinate | 32 min |
  | Oban | about a fifth after 30 min |
  | River | held no claims: its fetches hit the 10 s timeout |

- **Postgres memory:** Awa holds about 26 MB of private memory per connection. That comes from cached metadata and plans for its 220 per-slot tables.

## Tables

The rest of this file is `scripts/programme_sweep_report.py` over the three roots.


## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_tuned`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 8,550 | 1,541 | 483 | 33.0 | 548 |
| pgboss | 3,495 | 3,787 | 795 | 0.0 | 639,069 |
| river | 8,113 | 4,702 | 207 | 0.0 | 1,514,412 |
| oban | 5,673 | 6,447 | 6,143 | 0.0 | 1,045,237 |
| procrastinate | 1,472 | 2,470 | 5,868 | 0.0 | 172,468 |
| absurd | 1,382 | 2,641 | 2,795 | 0.0 | 92,160 |
| pgmq | 7,388 | 1,346 | 600 | 0.0 | 1,551,084 |
| pgque | 47,282 | 443 | 661 | 0.0 | 6,342 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.57 | 1.72 | 0.51 | 353 | 72 | 1.96 | 1.41 | 0.29 | 0.00 | 0.03 | 0.13 | 0.25 | 0.02 | 0 | 0.50 / 0.49 |
| pgboss | 3.94 | 4.03 | 0.77 | 377 | 340 | 38.32 | 5.25 | 14.50 | 0.00 | 16.13 | 0.86 | 15.97 | 0.22 | 0 | 0.93 / 0.51 |
| river | 1.05 | 1.16 | 0.52 | 46 | 20 | 1.70 | 0.80 | 0.00 | 0.00 | 0.09 | 0.29 | 0.00 | 0.00 | 0 | 0.49 / 0.51 |
| oban | 3.78 | 4.00 | 6.20 | 418 | 8,654 | 23.26 | 2.66 | 2.54 | 0.00 | 14.59 | 0.78 | 2.34 | 0.01 | 0 | 0.52 / 0.51 |
| procrastinate | 2.23 | 2.55 | 2.02 | 76 | 162 | 5.39 | 1.88 | 0.02 | 0.00 | 1.68 | 0.90 | 0.03 | 0.05 | 0 | 0.50 / 1.69 |
| absurd | 2.69 | 2.93 | 0.64 | 44 | 173 | 3.86 | 2.57 | 0.01 | 0.00 | 0.46 | 0.65 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgmq | 4.00 | 4.16 | 0.39 | 343 | 177 | 104.42 | 18.07 | 46.21 | 0.00 | 38.50 | 0.87 | 47.44 | 0.77 | 0 | 0.50 / 0.51 |
| pgque | 1.09 | 1.43 | 1.90 | 216 | 260 | 5.39 | 0.78 | 2.86 | 0.00 | 0.31 | 0.53 | 2.78 | 0.67 | 0 | 0.51 / 0.51 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_tuned_k4`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 10,762 | 1,619 | 1,541 | 33.0 | 708 |
| river | 10,905 | 5,251 | 755 | 0.0 | 2,195,052 |
| absurd | 1,625 | 3,212 | 3,531 | 0.0 | 40,780 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 3.96 | 4.02 | 0.83 | 456 | 77 | 5.01 | 3.65 | 0.73 | 0.00 | 0.04 | 0.19 | 0.75 | 0.03 | 0 | 0.53 / 0.49 |
| river | 2.16 | 2.75 | 0.74 | 55 | 22 | 4.41 | 1.87 | 0.00 | 0.00 | 1.17 | 0.72 | 0.00 | 0.00 | 0 | 0.51 / 0.96 |
| absurd | 3.99 | 4.02 | 0.95 | 103 | 173 | 12.55 | 8.28 | 0.05 | 0.00 | 2.78 | 0.89 | 0.05 | 0.00 | 0 | 0.50 / 0.51 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_tuned_k16`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| pgmq | 5,710 | 1,331 | 4,706 | 0.0 | 61,712 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| pgmq | 4.00 | 4.15 | 0.88 | 382 | 177 | 69.85 | 11.27 | 27.64 | 0.00 | 26.17 | 0.89 | 30.15 | 0.38 | 0 | 0.50 / 1.02 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w4`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 488 | 2,369 | 763 | 32.8 | 355 |
| pgboss | 512 | 2,545 | 89 | 0.0 | 58,316 |
| river | 79 | 3,702 | 41 | 0.0 | 9,500 |
| oban | 385 | 4,303 | 507 | 0.0 | 46,155 |
| procrastinate | 474 | 2,473 | 2,727 | 0.0 | 49,675 |
| absurd | 274 | 2,252 | 624 | 0.0 | 239 |
| pgmq | 3,494 | 1,702 | 254 | 0.0 | 447,939 |
| pgque | 3,215 | 428 | 38 | 0.0 | 438 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.34 | 1.43 | 0.15 | 297 | 12 | 1.47 | 1.04 | 0.04 | 0.00 | 0.09 | 0.15 | 0.04 | 0.01 | 0 | 0.51 / 0.50 |
| pgboss | 0.48 | 0.64 | 0.08 | 25 | 65 | 0.45 | 0.42 | 0.00 | 0.00 | 0.00 | 0.02 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| river | 0.05 | 0.07 | 0.02 | 25 | 10 | 0.07 | 0.03 | 0.00 | 0.00 | 0.00 | 0.02 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| oban | 0.22 | 0.27 | 0.36 | 46 | 2,125 | 0.68 | 0.12 | 0.00 | 0.00 | 0.27 | 0.19 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| procrastinate | 0.78 | 0.85 | 0.70 | 29 | 40 | 1.86 | 0.53 | 0.00 | 0.00 | 0.23 | 0.57 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| absurd | 0.82 | 0.87 | 0.13 | 19 | 45 | 0.97 | 0.80 | 0.00 | 0.00 | 0.00 | 0.13 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| pgmq | 0.29 | 0.37 | 0.13 | 28 | 43 | 0.51 | 0.22 | 0.00 | 0.00 | 0.14 | 0.11 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgque | 0.06 | 0.07 | 0.14 | 28 | 56 | 0.05 | 0.02 | 0.00 | 0.00 | 0.00 | 0.03 | 0.00 | 0.00 | 0 | 0.51 / 0.49 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w16`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 1,717 | 1,704 | 709 | 33.0 | 353 |
| pgboss | 2,048 | 2,770 | 217 | 0.0 | 243,272 |
| river | 317 | 3,772 | 39 | 0.0 | 37,600 |
| oban | 1,312 | 4,750 | 1,413 | 0.0 | 153,851 |
| procrastinate | 454 | 2,476 | 2,672 | 0.0 | 54,203 |
| absurd | 440 | 2,346 | 890 | 0.0 | 11,606 |
| pgmq | 12,013 | 2,053 | 581 | 0.0 | 2,143,975 |
| pgque | 9,751 | 432 | 51 | 0.0 | 725 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.44 | 1.50 | 0.20 | 345 | 21 | 1.47 | 1.27 | 0.00 | 0.00 | 0.00 | 0.07 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgboss | 1.95 | 2.50 | 0.32 | 48 | 92 | 2.11 | 1.75 | 0.00 | 0.00 | 0.12 | 0.13 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.06 | 0.10 | 0.03 | 22 | 14 | 0.05 | 0.02 | 0.00 | 0.00 | 0.00 | 0.02 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| oban | 0.58 | 0.66 | 1.36 | 76 | 2,160 | 1.00 | 0.23 | 0.00 | 0.00 | 0.55 | 0.15 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| procrastinate | 0.81 | 0.86 | 0.70 | 30 | 40 | 1.75 | 0.66 | 0.00 | 0.00 | 0.17 | 0.51 | 0.00 | 0.00 | 0 | 0.92 / 0.50 |
| absurd | 0.74 | 0.79 | 0.20 | 19 | 45 | 0.87 | 0.63 | 0.00 | 0.00 | 0.00 | 0.21 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgmq | 1.73 | 2.37 | 0.32 | 68 | 45 | 3.05 | 1.71 | 0.00 | 0.00 | 0.49 | 0.44 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgque | 0.12 | 0.14 | 0.32 | 31 | 56 | 0.16 | 0.09 | 0.00 | 0.00 | 0.01 | 0.06 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w64`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 5,689 | 1,524 | 635 | 32.7 | 585 |
| pgboss | 2,248 | 3,130 | 308 | 0.0 | 464,236 |
| river | 1,267 | 4,052 | 53 | 0.0 | 147,969 |
| oban | 3,993 | 5,399 | 3,951 | 0.0 | 557,559 |
| procrastinate | 459 | 2,465 | 2,727 | 0.0 | 54,569 |
| absurd | 578 | 2,470 | 1,147 | 0.0 | 33,967 |
| pgmq | 5,378 | 2,015 | 577 | 0.0 | 866,781 |
| pgque | 18,907 | 436 | 72 | 0.0 | 1,138 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.57 | 1.70 | 0.37 | 329 | 64 | 1.99 | 1.66 | 0.04 | 0.00 | 0.03 | 0.11 | 0.04 | 0.02 | 0 | 0.95 / 0.50 |
| pgboss | 2.89 | 3.17 | 0.35 | 104 | 201 | 2.77 | 2.15 | 0.00 | 0.00 | 0.19 | 0.18 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| river | 0.13 | 0.21 | 0.09 | 25 | 16 | 0.23 | 0.12 | 0.00 | 0.00 | 0.01 | 0.07 | 0.00 | 0.00 | 0 | 0.52 / 0.49 |
| oban | 1.55 | 1.79 | 4.13 | 258 | 2,200 | 11.03 | 0.80 | 0.01 | 0.00 | 8.51 | 0.39 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| procrastinate | 0.82 | 0.87 | 0.70 | 33 | 40 | 1.96 | 0.59 | 0.00 | 0.00 | 0.17 | 0.59 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| absurd | 0.66 | 0.71 | 0.25 | 19 | 45 | 0.79 | 0.51 | 0.00 | 0.00 | 0.00 | 0.25 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| pgmq | 3.94 | 4.12 | 0.27 | 200 | 50 | 41.57 | 9.87 | 3.81 | 0.00 | 7.39 | 19.79 | 4.95 | 0.27 | 0 | 0.84 / 0.52 |
| pgque | 0.21 | 0.23 | 0.62 | 31 | 56 | 0.25 | 0.14 | 0.00 | 0.00 | 0.01 | 0.10 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w128`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 8,573 | 1,513 | 534 | 33.0 | 570 |
| pgboss | 2,240 | 3,150 | 449 | 0.0 | 471,434 |
| river | 2,509 | 4,277 | 69 | 0.0 | 324,958 |
| oban | 5,418 | 5,580 | 5,565 | 0.0 | 910,516 |
| procrastinate | 451 | 2,464 | 2,549 | 0.0 | 52,611 |
| absurd | 614 | 2,521 | 1,209 | 0.0 | 32,479 |
| pgmq | 3,678 | 1,967 | 903 | 0.0 | 594,649 |
| pgque | 23,321 | 434 | 82 | 0.0 | 1,308 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.64 | 1.81 | 0.50 | 332 | 69 | 1.70 | 1.31 | 0.03 | 0.00 | 0.04 | 0.12 | 0.03 | 0.02 | 0 | 0.52 / 0.49 |
| pgboss | 3.00 | 3.29 | 0.36 | 128 | 188 | 2.89 | 2.00 | 0.00 | 0.00 | 0.25 | 0.25 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| river | 0.23 | 0.33 | 0.18 | 28 | 18 | 0.31 | 0.13 | 0.00 | 0.00 | 0.02 | 0.08 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| oban | 2.33 | 2.61 | 6.70 | 432 | 2,228 | 12.86 | 0.99 | 0.04 | 0.00 | 9.19 | 0.41 | 0.00 | 0.00 | 0 | 0.52 / 0.51 |
| procrastinate | 0.76 | 0.84 | 0.67 | 34 | 40 | 2.06 | 0.62 | 0.00 | 0.00 | 0.30 | 0.64 | 0.00 | 0.00 | 0 | 0.51 / 0.52 |
| absurd | 0.63 | 0.70 | 0.26 | 18 | 45 | 0.81 | 0.41 | 0.00 | 0.00 | 0.00 | 0.37 | 0.00 | 0.00 | 0 | 0.52 / 0.50 |
| pgmq | 4.00 | 4.37 | 0.27 | 345 | 52 | 73.07 | 25.76 | 15.51 | 0.00 | 30.01 | 0.85 | 15.97 | 0.49 | 0 | 0.50 / 0.51 |
| pgque | 0.25 | 0.26 | 0.76 | 30 | 56 | 0.33 | 0.14 | 0.00 | 0.00 | 0.05 | 0.14 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |

## Reference rate: 800 jobs/s offered, 32 workers

| system | jobs/s | e2e p99 ms | claim p99 ms | producer p99 ms | depth | WAL B/job |
|---|---:|---:|---:|---:|---:|---:|
| awa | 799 | 21.0 | 19.0 | 3.3 | 0 | 1,703 |
| pgboss | 827 | 426.0 | 409.5 | – | 140 | 2,804 |
| river | 634 | – | 43,710.0 | – | 34,987 | 4,569 |
| oban | 800 | – | 19.5 | – | 9 | 4,983 |
| procrastinate | 442 | – | 47,062.7 | – | 28,942 | 3,105 |
| absurd | 223 | 119,537.5 | 119,536.5 | – | 95,700 | 5,187 |
| pgmq | 803 | 14.0 | 7.0 | – | 1 | 1,465 |
| pgque | 804 | 108.5 | 107.0 | 1.9 | 0 | 457 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.61 | 0.70 | 0.11 | 308 | 12 | 0.85 | 0.67 | 0.03 | 0.00 | 0.00 | 0.06 | 0.03 | 0.01 | 0 | 0.51 / 0.51 |
| pgboss | 0.83 | 1.01 | 0.19 | 54 | 86 | 1.27 | 0.68 | 0.00 | 0.00 | 0.20 | 0.33 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.14 | 0.47 | 0.09 | 31 | 16 | 0.14 | 0.05 | 0.00 | 0.00 | 0.00 | 0.05 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| oban | 0.36 | 0.45 | 1.20 | 141 | 2,140 | 1.24 | 0.20 | 0.00 | 0.00 | 0.67 | 0.21 | 0.00 | 0.00 | 0 | 0.52 / 0.50 |
| procrastinate | 0.91 | 0.98 | 0.79 | 33 | 40 | 2.14 | 0.73 | 0.00 | 0.00 | 0.28 | 0.64 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| absurd | 1.04 | 1.23 | 0.39 | 39 | 47 | 1.52 | 0.90 | 0.00 | 0.00 | 0.07 | 0.51 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| pgmq | 0.59 | 0.79 | 0.40 | 94 | 46 | 1.77 | 0.38 | 0.00 | 0.00 | 0.60 | 0.74 | 0.00 | 0.00 | 0 | 0.51 / 0.56 |
| pgque | 0.07 | 0.10 | 0.05 | 28 | 44 | 0.15 | 0.05 | 0.00 | 0.00 | 0.04 | 0.06 | 0.00 | 0.00 | 0 | 1.56 / 0.49 |

## Retry storm: 30% transient / 1% poison failures, max attempts 5

| system | baseline jobs/s | storm good jobs/s | storm claim p99 ms | storm producer p99 ms | storm depth | backlog drain s | latency recovery s | peak dead tup |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 200 | 138 | 24 | 4 | 0 | 39 | 25 | 4,572 |
| pgboss | 200 | 132 | 8,326 | – | 1,658 | 9 | 115 | 29,980 |
| river | 200 | 138 | 52 | – | 70 | – | 5 | 32,291 |
| oban | 200 | 138 | 44 | – | 2 | 95 | 46 | 34,611 |
| procrastinate | 200 | 138 | 11 | – | 1 | 4 | 15 | 31,628 |
| absurd | 201 | 138 | 12 | – | 1 | 3 | 181 | 7,651 |
| pgmq | 201 | 138 | 42 | – | 1 | 125 | 2 | 26,908 |
| pgque | 201 | 138 | 107 | 2 | 0 | 245 | 2 | 5,089 |

Resources during `storm`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.75 | 2.96 | 0.12 | 725 | 15 | 3.07 | 2.63 | 0.07 | 0.01 | 0.07 | 0.10 | 0.08 | 0.33 | 0 | 1.00 / 0.50 |
| pgboss | 0.40 | 0.45 | 0.07 | 44 | 70 | 0.33 | 0.20 | 0.00 | 0.00 | 0.00 | 0.11 | 0.00 | 0.00 | 0 | 0.70 / 0.50 |
| river | 0.08 | 0.13 | 0.06 | 27 | 15 | 0.12 | 0.02 | 0.00 | 0.00 | 0.01 | 0.06 | 0.00 | 0.00 | 0 | 1.58 / 0.49 |
| oban | 0.21 | 0.29 | 0.92 | 72 | 2,127 | 0.56 | 0.13 | 0.00 | 0.00 | 0.10 | 0.24 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| procrastinate | 0.55 | 0.59 | 0.48 | 34 | 40 | 1.04 | 0.44 | 0.00 | 0.00 | 0.05 | 0.31 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| absurd | 0.68 | 0.76 | 0.24 | 35 | 47 | 0.89 | 0.54 | 0.00 | 0.00 | 0.03 | 0.29 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| pgmq | 0.34 | 0.40 | 0.13 | 60 | 45 | 0.61 | 0.23 | 0.00 | 0.00 | 0.11 | 0.25 | 0.00 | 0.00 | 0 | 0.81 / 0.50 |
| pgque | 0.18 | 0.19 | 0.04 | 33 | 45 | 0.23 | 0.12 | 0.00 | 0.00 | 0.00 | 0.09 | 0.00 | 0.00 | 0 | 0.49 / 0.50 |

## Queue fan-out: 200 jobs/s total across N queues, then idle — `fanout_q1`

| system | jobs/s | claim p99 ms | backends | LISTEN conns | idle xacts/s | idle PG cores |
|---|---:|---:|---:|---:|---:|---:|
| awa | 200 | 19 | 11 | 2 | 60 | 0.06 |
| pgboss | 200 | 5 | 19 | 0 | 16 | 0.05 |
| river | 200 | 51 | 5 | 0 | 8 | 0.02 |
| oban | 200 | 13 | 25 | 1 | 163 | 0.02 |
| procrastinate | 200 | 6 | 6 | 1 | 374 | 0.03 |
| absurd | 201 | 10 | 3 | 0 | 8 | 0.02 |
| pgmq | 201 | 4 | 19 | 1 | 36 | 0.03 |
| pgque | 200 | 137 | 22 | 1 | 35 | 0.03 |

Resources during `clean_1`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.57 | 0.72 | 0.08 | 327 | 8 | 1.05 | 0.80 | 0.05 | 0.00 | 0.08 | 0.06 | 0.05 | 0.02 | 0 | 0.52 / 0.49 |
| pgboss | 2.32 | 2.38 | 0.36 | 66 | 78 | 2.11 | 1.50 | 0.00 | 0.00 | 0.04 | 0.33 | 0.00 | 0.02 | 0 | 0.50 / 0.49 |
| river | 0.07 | 0.12 | 0.06 | 25 | 14 | 0.13 | 0.04 | 0.00 | 0.00 | 0.00 | 0.07 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| oban | 0.19 | 0.24 | 0.37 | 71 | 2,122 | 0.35 | 0.09 | 0.00 | 0.00 | 0.06 | 0.12 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| procrastinate | 0.47 | 0.55 | 0.45 | 26 | 40 | 0.99 | 0.45 | 0.00 | 0.00 | 0.04 | 0.27 | 0.00 | 0.00 | 0 | 0.69 / 0.51 |
| absurd | 0.58 | 0.62 | 0.20 | 32 | 47 | 0.82 | 0.45 | 0.00 | 0.00 | 0.05 | 0.28 | 0.00 | 0.00 | 0 | 0.52 / 0.48 |
| pgmq | 0.91 | 1.03 | 0.47 | 56 | 45 | 1.10 | 0.58 | 0.00 | 0.00 | 0.02 | 0.34 | 0.00 | 0.00 | 0 | 0.51 / 0.49 |
| pgque | 0.08 | 0.08 | 0.04 | 48 | 44 | 1.10 | 0.07 | 0.00 | 0.00 | 0.00 | 0.04 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |

## Queue fan-out: 200 jobs/s total across N queues, then idle — `fanout_q200`

| system | jobs/s | claim p99 ms | backends | LISTEN conns | idle xacts/s | idle PG cores |
|---|---:|---:|---:|---:|---:|---:|
| awa | 200 | 85 | 20 | 2 | 667 | 0.24 |
| pgboss | 200 | 5 | 6 | 0 | 49 | 0.98 |
| river | 200 | 42 | 15 | 0 | 203 | 0.05 |
| oban | 200 | 13 | 25 | 1 | 168 | 0.02 |
| procrastinate | 200 | 11 | 6 | 1 | 374 | 0.04 |
| absurd | 201 | 255 | 18 | 0 | 809 | 1.68 |
| pgmq | 201 | 3 | 19 | 1 | 401 | 0.11 |
| pgque | 200 | 105 | 22 | 1 | 1,498 | 1.05 |

Resources during `clean_1`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 2.28 | 3.04 | 0.40 | 578 | 13 | 2.62 | 1.75 | 0.03 | 0.00 | 0.28 | 0.23 | 0.03 | 0.26 | 0 | 0.50 / 0.50 |
| pgboss | 1.52 | 2.37 | 0.14 | 250 | 75 | 1.80 | 1.46 | 0.00 | 0.00 | 0.03 | 0.28 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| river | 0.20 | 0.27 | 0.17 | 57 | 36 | 0.33 | 0.11 | 0.00 | 0.00 | 0.03 | 0.12 | 0.00 | 0.00 | 0 | 1.04 / 0.50 |
| oban | 0.26 | 0.33 | 1.77 | 98 | 2,165 | 0.59 | 0.20 | 0.00 | 0.00 | 0.05 | 0.21 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| procrastinate | 0.61 | 0.67 | 0.53 | 27 | 41 | 1.23 | 0.55 | 0.00 | 0.00 | 0.08 | 0.36 | 0.00 | 0.00 | 0 | 1.03 / 0.50 |
| absurd | 2.16 | 2.56 | 0.33 | 121 | 48 | 2.45 | 1.93 | 0.00 | 0.00 | 0.10 | 0.31 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgmq | 0.67 | 0.84 | 0.41 | 109 | 45 | 0.91 | 0.47 | 0.00 | 0.00 | 0.02 | 0.29 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| pgque | 1.22 | 1.30 | 0.23 | 106 | 47 | 2.52 | 0.99 | 0.00 | 0.00 | 0.56 | 0.23 | 0.00 | 0.00 | 0 | 0.52 / 0.50 |

## Logical replication: FOR ALL TABLES publication, streaming then stalled consumer

| system | stream jobs/s | tables w/o RI | lag median B | stall retained WAL B | catalog_xmin age | catalog dead tup | WAL B/job |
|---|---:|---:|---:|---:|---:|---:|---:|
| awa | 399 | 0 | 302,520 | 200,520,832 | 44,141 | 11,374 | 2,008 |
| pgboss | 334 | 0 | 464,716 | 255,485,144 | 93,691 | 139 | 3,246 |
| river | 317 | 0 | 635,936 | 388,671,960 | 28,655 | 214 | 4,819 |
| oban | 400 | 0 | 363,312 | 569,002,728 | 164,143 | 115 | 5,017 |
| procrastinate | 395 | 0 | 167,832 | 347,823,136 | 328,164 | 25 | 2,868 |
| absurd | 402 | 0 | 618,244 | 189,640,600 | 118,551 | 22 | 3,228 |
| pgmq | 402 | 2 | 240,100 | 168,812,288 | 250,963 | 20 | 1,795 |
| pgque | 402 | 5 | 135,344 | 48,076,624 | 21,382 | 114 | 490 |

Resources during `stall_1`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.59 | 0.69 | 0.09 | 335 | 11 | 0.76 | 0.36 | 0.00 | 0.00 | 0.00 | 0.05 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgboss | 0.64 | 0.73 | 0.10 | 41 | 71 | 1.14 | 0.53 | 0.00 | 0.00 | 0.06 | 0.27 | 0.00 | 0.00 | 0 | 0.49 / 1.53 |
| river | 0.28 | 0.45 | 0.06 | 49 | 16 | 0.58 | 0.24 | 0.00 | 0.00 | 0.00 | 0.07 | 0.00 | 0.00 | 0 | 1.63 / 0.51 |
| oban | 0.27 | 0.38 | 0.74 | 80 | 2,136 | 0.85 | 0.18 | 0.01 | 0.00 | 0.19 | 0.16 | 0.00 | 0.00 | 0 | 0.52 / 0.50 |
| procrastinate | 0.87 | 0.95 | 0.68 | 43 | 40 | 2.07 | 0.64 | 0.00 | 0.00 | 0.13 | 0.64 | 0.00 | 0.00 | 0 | 0.49 / 1.56 |
| absurd | 1.09 | 1.22 | 0.22 | 43 | 47 | 1.56 | 0.95 | 0.00 | 0.00 | 0.08 | 0.26 | 0.00 | 0.00 | 0 | 0.52 / 0.51 |
| pgmq | 0.39 | 0.51 | 0.23 | 58 | 45 | 1.14 | 0.24 | 0.00 | 0.00 | 0.15 | 0.47 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgque | 0.07 | 0.09 | 0.04 | 38 | 46 | 0.33 | 0.05 | 0.00 | 0.00 | 0.00 | 0.03 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |

## Neighbour OLTP: pgbench 300 TPS on the same server (latency ms)

| system | baseline p99 | 1× p99 | 4× p99 | 4× service p99 | 4× tps ratio | 4× queue jobs/s |
|---|---:|---:|---:|---:|---:|---:|
| awa | 2.3 | 4.5 | 6.5 | 4.9 | 0.99 | 1,600 |
| pgboss | 2.1 | 4.5 | 10.1 | 5.1 | 1.01 | 512 |
| river | 2.2 | 8.9 | 7.4 | 4.7 | 1.00 | 317 |
| oban | 4.0 | 5.0 | 11.5 | 6.3 | 1.00 | 1,391 |
| procrastinate | 2.1 | 5.1 | 5.3 | 4.5 | 1.00 | 427 |
| absurd | 2.3 | 6.3 | 14.4 | 5.5 | 1.00 | 80 |
| pgmq | 2.1 | 4.6 | 18.3 | 6.3 | 1.00 | 1,607 |
| pgque | 2.1 | 3.2 | 2.4 | 2.3 | 1.00 | 1,606 |

Resources during `saturation`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.62 | 1.71 | 0.21 | 361 | 14 | 2.05 | 1.33 | 0.12 | 0.00 | 0.13 | 0.26 | 0.13 | 0.06 | 0 | 0.50 / 0.50 |
| pgboss | 1.09 | 1.23 | 0.19 | 58 | 71 | 1.73 | 0.86 | 0.03 | 0.00 | 0.24 | 0.41 | 0.03 | 0.09 | 0 | 1.56 / 0.50 |
| river | 0.45 | 0.70 | 0.08 | 56 | 15 | 0.63 | 0.16 | 0.01 | 0.00 | 0.07 | 0.26 | 0.01 | 0.01 | 0 | 1.67 / 0.49 |
| oban | 0.91 | 1.02 | 1.83 | 117 | 2,154 | 3.41 | 0.56 | 0.04 | 0.00 | 2.05 | 0.40 | 0.04 | 0.02 | 0 | 0.51 / 0.51 |
| procrastinate | 1.05 | 1.14 | 0.77 | 47 | 40 | 2.67 | 0.88 | 0.01 | 0.00 | 0.51 | 0.74 | 0.01 | 0.00 | 0 | 1.05 / 0.68 |
| absurd | 1.56 | 1.79 | 0.47 | 57 | 47 | 2.33 | 1.38 | 0.02 | 0.00 | 0.16 | 0.62 | 0.02 | 0.04 | 0 | 0.51 / 0.49 |
| pgmq | 0.73 | 1.21 | 0.40 | 83 | 45 | 2.27 | 0.44 | 0.02 | 0.00 | 0.79 | 0.83 | 0.03 | 0.01 | 0 | 1.55 / 0.50 |
| pgque | 0.24 | 0.26 | 0.08 | 42 | 45 | 0.51 | 0.20 | 0.02 | 0.00 | 0.05 | 0.18 | 0.01 | 0.02 | 0 | 0.50 / 0.50 |

## Scheduled herd: 20k jobs due at one instant, immediate stream at 200/s

| system | lateness p50 ms | lateness p99 ms | herd drain s | early starts | immediate claim p99 ms | immediate jobs/s |
|---|---:|---:|---:|---:|---:|---:|
| awa | 6,334 | 11,975 | 12.1 | 0 | 19 | 200 |
| pgboss | 39,316 | 77,366 | 78.3 | 0 | 53,107 | 256 |
| river | 31,634 | 62,592 | 63.2 | 0 | 9,210 | 200 |
| oban | 11,653 | 20,544 | 20.7 | 0 | 13 | 200 |
| procrastinate | 20,743 | 40,986 | 41.4 | 0 | 9 | 200 |
| absurd | 96,588 | 136,800 | 137.7 | 0 | 113,193 | 238 |
| pgmq | 958 | 1,938 | 2.0 | 0 | 43 | 201 |
| pgque | – | – | – | – | 107 | 201 |

Resources during `due`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.62 | 1.48 | 0.09 | 350 | 31 | 0.77 | 0.59 | 0.07 | 0.00 | 0.00 | 0.08 | 0.07 | 0.09 | 0 | 0.51 / 0.50 |
| pgboss | 0.36 | 0.46 | 0.07 | 39 | 71 | 0.32 | 0.23 | 0.00 | 0.00 | 0.00 | 0.08 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.11 | 0.18 | 0.07 | 29 | 16 | 0.15 | 0.06 | 0.00 | 0.00 | 0.00 | 0.05 | 0.00 | 0.00 | 0 | 1.61 / 0.49 |
| oban | 0.26 | 0.90 | 0.56 | 102 | 2,139 | 0.53 | 0.20 | 0.00 | 0.00 | 0.09 | 0.17 | 0.00 | 0.00 | 0 | 0.71 / 0.49 |
| procrastinate | 0.60 | 0.81 | 0.49 | 43 | 41 | 1.22 | 0.40 | 0.00 | 0.00 | 0.09 | 0.46 | 0.00 | 0.00 | 0 | 0.75 / 0.66 |
| absurd | 0.91 | 0.98 | 0.19 | 26 | 47 | 1.07 | 0.80 | 0.00 | 0.00 | 0.04 | 0.23 | 0.00 | 0.00 | 0 | 0.49 / 0.50 |
| pgmq | 0.26 | 0.32 | 0.14 | 61 | 46 | 0.37 | 0.19 | 0.00 | 0.00 | 0.02 | 0.14 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| pgque | 0.08 | 0.08 | 0.04 | 29 | 45 | 0.10 | 0.03 | 0.00 | 0.00 | 0.00 | 0.06 | 0.00 | 0.00 | 0 | 0.74 / 0.50 |

## Long jobs: 40 s jobs, 2 × 100 workers, steady state

| system | WAL B/s | xacts/s | tup updated/s | peak dead tup | duplicate completions |
|---|---:|---:|---:|---:|---:|
| awa | 27,929 | 115 | 24.2 | 618 | 0 |
| pgboss | 6,518 | 260 | 4.8 | 295 | 0 |
| river | 11,465 | 52 | 6.1 | 352 | 360 |
| oban | 11,244 | 16 | 5.5 | 364 | 298 |
| procrastinate | 5,953 | 103 | 4.5 | 225 | 0 |
| absurd | 12,321 | 14 | 17.0 | 264 | 1,134 |
| pgmq | 4,638 | 244 | 5.9 | 501 | 610 |
| pgque | 6,255 | 47 | 0.7 | 78 | -842 |

Resources during `steady`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.11 | 0.23 | 0.02 | 333 | 13 | 0.26 | 0.04 | 0.00 | 0.00 | 0.00 | 0.19 | 0.00 | 0.00 | 0 | 0.92 / 0.50 |
| pgboss | 0.15 | 0.21 | 0.04 | 67 | 102 | 0.09 | 0.03 | 0.00 | 0.00 | 0.02 | 0.04 | 0.00 | 0.00 | 0 | 0.51 / 0.50 |
| river | 0.02 | 0.03 | 0.02 | 32 | 19 | 0.06 | 0.02 | 0.00 | 0.00 | 0.00 | 0.03 | 0.00 | 0.00 | 0 | 0.52 / 0.50 |
| oban | 0.03 | 0.04 | 0.25 | 477 | 4,250 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| procrastinate | 0.07 | 0.08 | 0.05 | 53 | 73 | 0.29 | 0.08 | 0.00 | 0.00 | 0.00 | 0.16 | 0.00 | 0.00 | 0 | 0.51 / 0.48 |
| absurd | 0.02 | 0.04 | 0.02 | 27 | 71 | 0.26 | 0.03 | 0.00 | 0.00 | 0.02 | 0.21 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgmq | 0.10 | 0.73 | 0.05 | 478 | 73 | 0.20 | 0.02 | 0.00 | 0.00 | 0.09 | 0.05 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| pgque | 0.03 | 0.04 | 0.01 | 44 | 64 | 0.03 | 0.01 | 0.01 | 0.00 | 0.00 | 0.01 | 0.01 | 0.00 | 0 | 0.52 / 0.50 |

## Backlog drain: 200k jobs preloaded behind a gate, then drained

| system | drain jobs/s | drain s | tail/head rate | DB MB baseline | DB MB peak | DB MB final |
|---|---:|---:|---:|---:|---:|---:|
| awa | 5,154 | 39 | 0.84 | 20.7 | 173.3 | 21.5 |
| pgboss | 2,510 | 72 | 1.00 | 8.4 | 204.5 | 204.5 |
| river | 1,226 | 163 | 0.99 | 7.9 | 216.2 | 216.2 |
| oban | 4,206 | 47 | 1.53 | 7.8 | 323.1 | 323.1 |
| procrastinate | 579 | 238 | 1.01 | 7.8 | 92.1 | 92.1 |
| absurd | – | – | 1.14 | 7.8 | 211.0 | 211.0 |
| pgmq | 14,539 | 14 | 1.14 | 7.9 | 179.5 | 97.3 |
| pgque | 14,299 | 14 | 1.36 | 8.7 | 75.9 | 75.9 |

Resources during `drain`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.19 | 1.54 | 0.04 | 267 | 27 | 0.23 | 0.18 | 0.00 | 0.00 | 0.00 | 0.03 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgboss | 0.33 | 0.68 | 0.06 | 39 | 81 | 0.15 | 0.12 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.09 | 0.28 | 0.04 | 25 | 16 | 0.04 | 0.03 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| oban | 0.23 | 1.68 | 0.54 | 184 | 2,188 | 0.73 | 0.07 | 0.00 | 0.00 | 0.54 | 0.06 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| procrastinate | 0.49 | 0.81 | 0.39 | 34 | 40 | 1.00 | 0.35 | 0.00 | 0.00 | 0.06 | 0.38 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| absurd | 0.92 | 1.05 | 0.10 | 41 | 47 | 1.03 | 0.83 | 0.00 | 0.00 | 0.00 | 0.18 | 0.00 | 0.00 | 0 | 0.49 / 0.49 |
| pgmq | 0.62 | 3.99 | 0.17 | 170 | 47 | 1.09 | 0.43 | 0.02 | 0.00 | 0.49 | 0.03 | 0.03 | 0.04 | 0 | 0.49 / 0.50 |
| pgque | 0.04 | 0.26 | 0.02 | 28 | 43 | 0.07 | 0.03 | 0.00 | 0.00 | 0.00 | 0.04 | 0.00 | 0.00 | 0 | 0.49 / 0.50 |

## Large payloads: 64 KiB incompressible at 50 jobs/s, then drain

| system | jobs/s | WAL B/job | TOAST MB after | DB MB after |
|---|---:|---:|---:|---:|
| awa | 50 | 75,272 | 0.6 | 22.2 |
| pgboss | 50 | 84,170 | 1,174.0 | 1,190.1 |
| river | 50 | 80,992 | 1,174.0 | 1,191.3 |
| oban | 50 | 83,078 | 1,173.8 | 1,192.2 |
| procrastinate | 50 | 80,650 | 1,163.1 | 1,179.5 |
| absurd | 50 | 86,672 | 1,174.0 | 1,193.4 |
| pgmq | 50 | 205,007 | 1,176.9 | 1,187.8 |
| pgque | 50 | 70,853 | 1,177.4 | 1,188.6 |

Resources during `clean_1`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.61 | 0.71 | 0.08 | 316 | 11 | 0.80 | 0.59 | 0.04 | 0.00 | 0.00 | 0.06 | 0.04 | 0.03 | 0 | 0.72 / 0.49 |
| pgboss | 0.17 | 0.20 | 0.06 | 44 | 72 | 0.18 | 0.11 | 0.00 | 0.00 | 0.00 | 0.06 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| river | 0.12 | 0.14 | 0.07 | 28 | 14 | 0.20 | 0.09 | 0.00 | 0.00 | 0.01 | 0.06 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| oban | 0.16 | 0.19 | 0.39 | 73 | 2,120 | 0.33 | 0.11 | 0.00 | 0.00 | 0.01 | 0.14 | 0.00 | 0.00 | 0 | 0.50 / 0.48 |
| procrastinate | 0.23 | 0.28 | 0.22 | 33 | 39 | 0.46 | 0.21 | 0.00 | 0.00 | 0.01 | 0.13 | 0.00 | 0.00 | 0 | 0.51 / 0.49 |
| absurd | 0.20 | 0.23 | 0.08 | 19 | 41 | 0.33 | 0.16 | 0.00 | 0.00 | 0.02 | 0.12 | 0.00 | 0.00 | 0 | 0.84 / 0.50 |
| pgmq | 0.18 | 0.23 | 0.11 | 63 | 45 | 0.49 | 0.10 | 0.00 | 0.00 | 0.08 | 0.15 | 0.00 | 0.00 | 0 | 0.49 / 0.88 |
| pgque | 0.14 | 0.27 | 0.05 | 30 | 41 | 0.19 | 0.12 | 0.00 | 0.00 | 0.02 | 0.06 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |

## Large incompressible args (64 KiB / 1 MiB saturation; 8 GiB of 1 MiB jobs preloaded and drained) — `backlog_1m`

| system | jobs/s | producer call p99 ms | WAL B/job | worker MB peak | preloaded jobs | drain jobs/s | drain worker MB peak | TOAST MB after | DB MB peak | DB MB final |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | – | – | – | – | 6,494 | 169.5 | 259 | 0.6 | 6,774 | 21 |
| pgboss | – | – | – | – | 8,004 | 616.3 | 5,319 | 8,330.5 | 8,346 | 8,346 |
| river | – | – | – | – | 7,680 | 279.1 | 379 | 7,983.4 | 7,997 | 7,997 |
| oban | – | – | – | – | 7,675 | 257.5 | 8,713 | 7,984.3 | 8,001 | 8,001 |
| procrastinate | – | – | – | – | 7,056 | 183.7 | 216 | 7,341.0 | 7,353 | 7,353 |
| absurd | – | – | – | – | 7,684 | 243.4 | 366 | 7,989.3 | 8,003 | 8,003 |
| pgmq | – | – | – | – | 7,684 | 45.2 | 998 | 16,012.4 | 16,023 | 16,022 |
| pgque | – | – | – | – | 7,994 | 231.5 | 217 | 8,316.9 | 8,331 | 8,331 |

Resources during `drain`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.17 | 1.11 | 0.05 | 341 | 259 | 0.69 | 0.13 | 0.01 | 0.00 | 0.03 | 0.32 | 0.01 | 0.02 | 0 | 0.92 / 0.49 |
| pgboss | 0.17 | 2.61 | 0.10 | 167 | 5,319 | 1.32 | 0.24 | 0.01 | 0.00 | 0.04 | 0.99 | 0.01 | 0.60 | 0 | 1.02 / 0.92 |
| river | 0.26 | 2.84 | 0.11 | 56 | 379 | 0.43 | 0.25 | 0.00 | 0.00 | 0.00 | 0.18 | 0.00 | 0.00 | 0 | 0.50 / 0.91 |
| oban | 0.20 | 2.31 | 0.19 | 237 | 8,713 | 0.57 | 0.19 | 0.08 | 0.00 | 0.08 | 0.20 | 0.08 | 0.14 | 0 | 0.92 / 1.04 |
| procrastinate | 0.25 | 1.84 | 0.21 | 100 | 216 | 0.53 | 0.21 | 0.01 | 0.00 | 0.03 | 0.22 | 0.01 | 0.01 | 0 | 1.02 / 0.91 |
| absurd | 0.25 | 1.30 | 0.10 | 48 | 366 | 0.46 | 0.19 | 0.00 | 0.00 | 0.03 | 0.24 | 0.00 | 0.01 | 0 | 0.51 / 1.63 |
| pgmq | 0.70 | 1.59 | 0.15 | 338 | 998 | 26.61 | 0.35 | 0.32 | 0.00 | 23.15 | 1.95 | 0.34 | 0.56 | 0 | 0.49 / 0.95 |
| pgque | 0.25 | 1.19 | 0.11 | 85 | 217 | 0.84 | 0.16 | 0.30 | 0.00 | 0.03 | 0.35 | 0.29 | 0.03 | 0 | 0.94 / 0.66 |

## Large incompressible args (64 KiB / 1 MiB saturation; 8 GiB of 1 MiB jobs preloaded and drained) — `sat_65536`

| system | jobs/s | producer call p99 ms | WAL B/job | worker MB peak | preloaded jobs | drain jobs/s | drain worker MB peak | TOAST MB after | DB MB peak | DB MB final |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 691 | 367 | 94,792 | 88 | – | – | – | – | 1,689 | 1,218 |
| pgboss | 563 | – | 73,642 | 366 | – | – | – | – | 6,370 | 6,370 |
| river | 798 | – | 86,529 | 53 | – | – | – | – | 12,029 | 12,029 |
| oban | 664 | – | 102,726 | 2,255 | – | – | – | – | 9,920 | 9,920 |
| procrastinate | 199 | – | 109,605 | 41 | – | – | – | – | 1,893 | 1,893 |
| absurd | 269 | – | 113,468 | 60 | – | – | – | – | 3,306 | 3,306 |
| pgmq | 424 | – | 188,197 | 233 | – | – | – | – | 24,675 | 24,675 |
| pgque | 383 | 750 | 71,025 | 99 | – | – | – | – | 3,905 | 3,905 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.37 | 1.52 | 0.21 | 294 | 88 | 2.28 | 1.02 | 0.06 | 0.00 | 0.47 | 0.35 | 0.06 | 0.04 | 0 | 0.51 / 1.11 |
| pgboss | 0.85 | 1.01 | 0.33 | 116 | 366 | 3.54 | 0.59 | 0.01 | 0.00 | 2.09 | 0.37 | 0.01 | 0.05 | 0 | 1.65 / 1.91 |
| river | 0.97 | 1.30 | 0.40 | 43 | 53 | 2.95 | 0.76 | 0.00 | 0.00 | 1.07 | 0.66 | 0.00 | 0.00 | 0 | 0.49 / 1.52 |
| oban | 1.14 | 1.49 | 1.12 | 407 | 2,255 | 18.85 | 0.69 | 0.00 | 0.00 | 16.77 | 0.71 | 0.00 | 0.00 | 0 | 0.49 / 1.91 |
| procrastinate | 0.74 | 0.87 | 0.61 | 35 | 41 | 2.19 | 0.50 | 0.00 | 0.00 | 0.45 | 0.54 | 0.00 | 0.00 | 0 | 0.93 / 0.51 |
| absurd | 0.61 | 0.86 | 0.21 | 21 | 60 | 1.64 | 0.40 | 0.00 | 0.00 | 0.13 | 0.52 | 0.00 | 0.00 | 0 | 0.91 / 1.14 |
| pgmq | 1.55 | 2.56 | 0.43 | 301 | 233 | 17.46 | 1.05 | 0.14 | 0.00 | 14.66 | 0.88 | 0.13 | 0.22 | 0 | 1.64 / 4.27 |
| pgque | 0.81 | 0.90 | 0.26 | 77 | 99 | 1.26 | 0.68 | 0.00 | 0.00 | 0.29 | 0.29 | 0.00 | 0.00 | 0 | 1.60 / 0.90 |

## Large incompressible args (64 KiB / 1 MiB saturation; 8 GiB of 1 MiB jobs preloaded and drained) — `sat_1048576`

| system | jobs/s | producer call p99 ms | WAL B/job | worker MB peak | preloaded jobs | drain jobs/s | drain worker MB peak | TOAST MB after | DB MB peak | DB MB final |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 51 | 3,596 | 1,415,769 | 944 | – | – | – | – | 1,627 | 823 |
| pgboss | 26 | – | 1,494,355 | 552 | – | – | – | – | 7,246 | 7,246 |
| river | 51 | – | 1,240,379 | 531 | – | – | – | – | 19,563 | 19,563 |
| oban | 45 | – | 1,203,764 | 2,550 | – | – | – | – | 10,255 | 10,255 |
| procrastinate | 22 | – | 1,957,641 | 57 | – | – | – | – | 3,442 | 3,442 |
| absurd | 38 | – | 1,274,797 | 372 | – | – | – | – | 15,860 | 15,860 |
| pgmq | 26 | – | 3,624,393 | 3,076 | – | – | – | – | 16,992 | 16,992 |
| pgque | 25 | 5,852 | 1,120,776 | 808 | – | – | – | – | 4,052 | 4,052 |

Resources during `clean`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.15 | 1.26 | 0.21 | 261 | 944 | 2.11 | 1.03 | 0.01 | 0.00 | 0.31 | 0.35 | 0.01 | 0.00 | 0 | 0.92 / 0.92 |
| pgboss | 0.77 | 0.96 | 0.31 | 800 | 552 | 2.10 | 0.65 | 0.00 | 0.00 | 0.41 | 0.45 | 0.00 | 0.00 | 0 | 1.60 / 1.65 |
| river | 0.94 | 1.31 | 0.31 | 171 | 531 | 2.66 | 0.74 | 0.00 | 0.00 | 1.00 | 0.66 | 0.00 | 0.00 | 0 | 0.93 / 4.24 |
| oban | 0.81 | 1.07 | 0.47 | 841 | 2,550 | 3.59 | 0.58 | 0.00 | 0.00 | 2.02 | 0.52 | 0.00 | 0.00 | 0 | 0.94 / 1.17 |
| procrastinate | 0.58 | 0.65 | 0.57 | 41 | 57 | 2.14 | 0.40 | 0.00 | 0.00 | 0.46 | 0.42 | 0.00 | 0.00 | 0 | 0.93 / 1.08 |
| absurd | 0.62 | 1.03 | 0.16 | 30 | 372 | 2.52 | 0.52 | 0.00 | 0.00 | 0.65 | 0.98 | 0.00 | 0.00 | 0 | 0.92 / 1.13 |
| pgmq | 1.00 | 1.94 | 0.38 | 1,180 | 3,076 | 6.28 | 0.81 | 0.00 | 0.00 | 3.88 | 0.62 | 0.01 | 0.00 | 0 | 0.93 / 1.17 |
| pgque | 0.77 | 0.87 | 0.26 | 567 | 808 | 1.30 | 0.65 | 0.00 | 0.00 | 0.37 | 0.28 | 0.00 | 0.00 | 0 | 4.15 / 0.92 |

## Idle background cost (workers running, no jobs)

| system | xacts/s | WAL B/s | relfile swaps/s | PG cores |
|---|---:|---:|---:|---:|
| awa | 60.1 | 3,909 | 0.37 | 0.053 |
| pgboss | 21.9 | 1,770 | 0.00 | 0.031 |
| river | 24.0 | 11,387 | 0.00 | 0.023 |
| oban | 16.0 | 4,190 | 0.00 | 0.015 |
| procrastinate | 65.5 | 459 | 0.00 | 0.068 |
| absurd | 23.5 | 991 | 0.00 | 0.051 |
| pgmq | 160.3 | 429 | 0.00 | 0.059 |
| pgque | 40.6 | 7,667 | 0.00 | 0.029 |

Resources during `idle_1`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 0.05 | 0.10 | 0.01 | 276 | 7 | 0.06 | 0.04 | 0.00 | 0.00 | 0.00 | 0.01 | 0.00 | 0.00 | 0 | 0.51 / 0.51 |
| pgboss | 0.03 | 0.04 | 0.00 | 25 | 62 | 0.02 | 0.02 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.02 | 0.04 | 0.01 | 23 | 10 | 0.01 | 0.01 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 1.56 / 0.50 |
| oban | 0.02 | 0.04 | 0.02 | 59 | 2,116 | 0.01 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| procrastinate | 0.07 | 0.08 | 0.03 | 33 | 37 | 0.05 | 0.04 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| absurd | 0.05 | 0.06 | 0.02 | 18 | 39 | 0.03 | 0.03 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.49 / 0.51 |
| pgmq | 0.06 | 0.07 | 0.02 | 38 | 37 | 0.04 | 0.04 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgque | 0.03 | 0.03 | 0.01 | 29 | 36 | 0.06 | 0.01 | 0.00 | 0.00 | 0.01 | 0.04 | 0.00 | 0.00 | 0 | 0.66 / 0.50 |

## Postgres restart under load (2 replicas, 400 jobs/s)

| system | baseline jobs/s | restart jobs/s | recovery jobs/s | workers back s | backlog clear s | recovery e2e p99 ms | recovery depth |
|---|---:|---:|---:|---:|---:|---:|---:|
| awa | 798 | 253 | 802 | 5 | 10 | 30 | 4 |
| pgboss | 668 | 221 | 668 | 5 | – | 400 | 176 |
| river | 634 | 219 | 634 | 5 | – | – | 37,708 |
| oban | 793 | 258 | 802 | 5 | 5 | – | 0 |
| procrastinate | 777 | 0 | 775 | 5 | 5 | – | 2 |
| absurd | 794 | 336 | 802 | 5 | 10 | 1,198 | 6 |
| pgmq | 401 | 146 | 399 | 5 | 5 | 28 | 1 |
| pgque | 801 | 244 | 805 | 5 | 5 | 109 | 0 |

Resources during `recovery`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 1.19 | 1.28 | 0.17 | 311 | 18 | 1.77 | 1.35 | 0.07 | 0.04 | 0.01 | 0.12 | 0.13 | 0.01 | 0 | 0.51 / 0.50 |
| pgboss | 0.94 | 1.03 | 0.21 | 75 | 139 | 1.29 | 0.69 | 0.00 | 0.00 | 0.11 | 0.42 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| river | 0.20 | 0.27 | 0.13 | 51 | 27 | 0.31 | 0.07 | 0.00 | 0.00 | 0.04 | 0.14 | 0.00 | 0.00 | 0 | 1.68 / 1.05 |
| oban | 0.46 | 0.56 | 1.75 | 122 | 4,261 | 1.54 | 0.23 | 0.00 | 0.00 | 0.90 | 0.28 | 0.00 | 0.00 | 0 | 0.50 / 0.49 |
| procrastinate | 1.35 | 1.44 | 1.40 | 62 | 82 | 3.38 | 1.01 | 0.04 | 0.00 | 0.81 | 0.82 | 0.05 | 0.01 | 0 | 0.51 / 1.06 |
| absurd | 1.14 | 1.25 | 0.62 | 25 | 94 | 2.25 | 0.99 | 0.00 | 0.00 | 0.49 | 0.70 | 0.00 | 0.00 | 0 | 0.50 / 0.51 |
| pgmq | 0.44 | 0.49 | 0.29 | 86 | 93 | 0.87 | 0.25 | 0.00 | 0.00 | 0.10 | 0.46 | 0.00 | 0.00 | 0 | 0.50 / 0.50 |
| pgque | 0.13 | 0.13 | 0.09 | 43 | 91 | 0.19 | 0.09 | 0.00 | 0.00 | 0.01 | 0.07 | 0.00 | 0.00 | 0 | 1.01 / 0.49 |

## Mass abandonment: fleet SIGKILLed holding every claim, fresh fleet restarted

| system | claimed at kill (DB) | held at kill (workers) | claim s | first rescue s | 50% s | 99% s | 100% s | rescue/s 10–90% | missing | duplicates | fresh claim p99 ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 999,885 | 999,869 | 46 | 141 | 741 | 1,226 | 1,231 | 919 | 0 | 0 | 117 |
| pgboss | 1,104,000 | 1,104,000 | – | 6 | 386 | 836 | 841 | 1,533 | 0 | 80 | 43 |
| river | 0 | 0 | – | 117 | 252 | 382 | 382 | 3,994 | 0 | 387 | 386 |
| oban | 998,387 | 998,374 | 37 | 127 | – | – | – | – | 798,438 | 0 | 2,470 |
| procrastinate | 910,864 | 910,852 | 587 | 316 | 1,142 | 1,902 | 1,917 | 558 | 0 | 0 | 32 |
| absurd | 998,434 | 998,434 | – | 131 | 717 | 1,694 | 1,709 | 593 | 0 | 43 | 99,120 |
| pgmq | 998,528 | 998,528 | 84 | 123 | 223 | 348 | 353 | 4,436 | 0 | 102 | 36 |
| pgque | 999,809 | 999,810 | 2 | 11 | 11 | 11 | 16 | – | 0 | 12 | 106 |

Resources during `rescue`:

| system | PG cores | PG cores peak | worker cores | PG anon MB peak | worker MB peak | AAS | AAS CPU | AAS Lock | AAS advisory | AAS LWLock | AAS IO | lock waiters | max lock wait s | deadlocks | commit p50 ms before / after |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 3.89 | 3.98 | 0.21 | 967 | 178 | 4.42 | 2.81 | 0.24 | 0.39 | 0.18 | 0.23 | 0.68 | 3.72 | 0 | 0.51 / 0.51 |
| pgboss | 2.88 | 4.20 | 0.25 | 258 | 274 | 13.39 | 8.41 | 0.17 | 0.95 | 2.41 | 0.89 | 1.12 | 18.49 | 0 | 0.50 / 0.50 |
| river | 3.57 | 4.83 | 0.54 | 1,759 | 209 | 91.06 | 26.96 | 2.39 | 0.00 | 43.99 | 1.52 | 2.49 | 119.51 | 0 | 0.50 / 0.50 |
| oban | 1.87 | 3.98 | 1.26 | 815 | 8,707 | 7.50 | 1.52 | 0.08 | 0.00 | 3.97 | 1.06 | 0.10 | 0.18 | 0 | 0.51 / 1.58 |
| procrastinate | 1.89 | 4.00 | 1.14 | 108 | 1,384 | 3.40 | 1.57 | 0.06 | 0.00 | 0.56 | 0.64 | 0.06 | 3.10 | 0 | 0.50 / 0.51 |
| absurd | 3.39 | 4.01 | 0.29 | 156 | 189 | 4.29 | 3.00 | 0.12 | 0.00 | 0.34 | 0.62 | 0.12 | 0.19 | 0 | 0.50 / 0.50 |
| pgmq | 1.30 | 4.14 | 0.45 | 355 | 183 | 18.40 | 4.30 | 6.04 | 0.00 | 7.16 | 0.35 | 6.46 | 2.19 | 0 | 0.50 / 0.49 |
| pgque | 0.16 | 0.20 | 0.10 | 78 | 1,062 | 0.36 | 0.11 | 0.14 | 0.00 | 0.02 | 0.08 | 0.13 | 0.00 | 0 | 0.50 / 0.50 |

