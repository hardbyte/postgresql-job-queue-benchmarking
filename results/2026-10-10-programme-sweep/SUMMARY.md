# 2026-10-10 programme sweep — eight queues, twelve scenarios, PostgreSQL 18.6

Cross-system comparison run on one host over 10–12 October 2026 (UTC), at
shortened phase lengths so the whole matrix fits in one pass:
`scripts/run_programme_sweep.sh`, rendered by
`scripts/programme_sweep_report.py`. 48 cells, all exit 0. Per-cell
`manifest.json` and `summary.json` are under `cells/`; `run_index.tsv`
maps each cell to them (`raw_run_dir` names the untracked raw directory).

Systems: awa, pg-boss, river, oban, procrastinate, absurd (job queues),
pgmq (visibility-timeout queue), pgque (event bus driven as a
single-consumer queue). Same `postgres:18.6-alpine`, same `postgres.conf`,
Postgres capped at 4 CPUs. awa is built from the queue-storage performance
stack as it stood on 10 October (`b3bf4cf`: main `4f7e79b` plus the branches
since merged as #510, #514, #512, plus #509 replica identity), with the
stock sqlx 0.9.0 (no TCP_NODELAY patch).

## Headlines

- **Reference rate (800 jobs/s, 32 workers):** awa 799/s at e2e p99 44 ms,
  depth 0, 1,118 WAL bytes/job. Lowest tail and WAL per job of the job
  queues; pgmq is lower still at 22 ms. pg-boss 336 ms, oban 161 ms claim
  p99; river, procrastinate and absurd fall behind the offered rate.
- **Saturation:** awa 370 / 1,263 / 4,614 / 8,310 jobs/s at 4 / 16 / 64 /
  128 workers, with peak dead tuples of 59–214 against 10k–1M for the other
  systems. pg-boss beats awa at 4 and 16 workers (512 / 2,048) and pgmq at
  every width up to 64; pgque is in a different contract and leads
  throughout. awa's gap at low worker counts is the per-claim floor (one
  claim round trip per freed worker permit), which #513 and the P7
  coalescing proposal target.
- **Retry storm:** awa holds 138 good jobs/s (the ceiling at 30% transient
  failures), claim p99 33 ms, depth 0, backlog drained in 40 s. pg-boss
  reaches 8.3 s claim p99 with a 1,664-deep backlog; absurd collapses.
- **Queue fan-out (200 queues):** awa keeps 200/s at claim p99 205 ms with 2
  LISTEN connections (the shared listener from #510); pgque opens 200
  LISTEN backends and drops to 153/s; procrastinate and absurd exceed 10 s.
  awa's idle cost with 200 queues is still 718 transactions/s and 0.44
  Postgres cores (poll back-off caps at 2 s per queue), the largest residual
  of this build.
- **Logical replication (FOR ALL TABLES, stalled consumer):** awa has no
  tables without replica identity (#509) and retains 140 MB of WAL during
  the stall, second only to pgque. It also accumulates 8,126 dead catalog
  tuples under the stalled slot where every other system stays under 220:
  ring rotation and prune are DDL, and a stalled slot pins the catalog rows
  they churn. #512 reduced the truncation rate; the remaining churn is the
  rotation ledger and the children that do hold rows.
- **Neighbour OLTP (pgbench 300 TPS alongside):** at 4× load awa drives
  1,398 jobs/s and raises the neighbour's p99 from 5 ms to 69 ms; pgmq
  (1,607/s) to 77 ms; pgque (1,591/s) to 6 ms. The neighbour's throughput is
  unaffected in every case (tps ratio 0.99–1.01).
- **Scheduled herd (20k due at once):** awa drains the herd in 15.3 s with
  lateness p99 15.2 s and the immediate stream unaffected (claim p99 35 ms);
  oban matches it; pgmq is 2.2 s. pg-boss and procrastinate starve the
  immediate stream (claim p99 above 50 s).
- **Long jobs (40 s, 200 workers):** awa writes 20.5 KB/s of WAL and 108
  transactions/s at steady state against 5–11 KB/s for the others, with 13.5
  tuples updated/s: heartbeat amplification, addressed by the heartbeat
  rewrite in #515. awa reports zero duplicate completions; river, oban,
  absurd and pgmq report hundreds.
- **Backlog drain (200k preloaded):** awa drains at 4,786/s and the
  database returns from 159 MB to 23 MB; pg-boss, river, oban and absurd
  keep their peak size. pgmq drains fastest (13,228/s) and returns to 96 MB.
- **Large payloads (64 KiB):** after the drain awa's TOAST footprint is
  0.6 MB; every other system keeps about 1.17 GB.
- **Postgres restart under load:** awa recovers to 1,038/s with e2e p99
  14 s during recovery and a 396-job backlog; pg-boss recovers similarly;
  oban, procrastinate and absurd produce nothing after the restart in this
  cell.

## How this compares with earlier runs

The May and August sweeps ran on a different host (24 CPU, 98 GB, kernel
6.18, Docker 29, PostgreSQL 18.3). This host is a Ryzen 9 5900X (24 threads,
62 GB, kernel 6.8, Docker 26.1.4, PostgreSQL 18.6) with other development
containers resident, and parts of the sweep overlapped with Rust builds and
test runs on the same box (niced, four build jobs).

| cell | May 2026 (other host) | this sweep |
|---|---:|---:|
| awa W=4 / 16 / 64 / 128 | 427 / 1,729 / 5,369 / 8,499 | 370 / 1,263 / 4,614 / 8,310 |
| pg-boss W=4 / 16 / 64 / 128 | 512 / 2,048 / 2,387 / 2,387 | 512 / 2,048 / 2,478 / 2,509 |
| pgmq W=4 / 16 / 64 / 128 | 3,571 / 11,277 / 10,243 / 6,180 | 3,046 / 6,610 / 6,493 / 2,907 |
| pgque W=4 / 16 / 64 / 128 | 3,439 / 11,505 / 27,719 / 34,433 | 3,060 / 8,804 / 19,930 / 25,178 |
| awa ref800 e2e p99 | 21 ms (August) | 44 ms |

pg-boss is identical because its throughput is set by its own polling
cadence, not the server. The systems that scale with the server are 15–40%
lower here, consistent with a slower host rather than a regression; the
August awa ref800 cell used the TCP_NODELAY-patched sqlx and a quiet
machine. The May headline of 14,158 jobs/s for awa was a 256-worker cell;
this sweep stopped at 128. A 256-worker cell and a same-host A/B of awa at
main (pre-#510), the merged stack, and the open #513 and #515 branches are
in `results/2026-10-12-post-sweep` (section below, filled in when complete).

## Known caveats in this run

- The "idle background cost" section is mislabelled: its `idle-background`
  phase kept the 200 jobs/s producer rate, so it is a 200/s steady-state
  cell, not an idle one. True idle numbers are the `idle_1` columns of the
  fan-out tables (rate 0). At 200/s awa still performs 21.6 relfilenode
  swaps/s with the sweep build; the queue ring rotates every few seconds at
  that rate and every rotation truncates the children that received rows.
- oban at 64 and 128 workers scored near zero because its Ecto pool was
  fixed at 20 connections; the adapter now sizes the pool from the worker
  count and those two cells are re-run in the post-sweep set.
- Three retry-storm teardowns (absurd, pg-boss, river) hung on the old
  harness code for hours before being killed by hand; their recorded phases
  are complete, but the sweep's wall time is not representative. The pacer
  and escalation fixes are in `bench_harness` since b957194.
- Postgres is capped at 4 CPUs; the host had other containers resident.

## Tables

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w128`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 8,310 | 979 | 433 | 11.1 | 214 |
| pgboss | 2,509 | 2,422 | 453 | 0.0 | 471,249 |
| river | 2,433 | 4,249 | 69 | 0.0 | 317,088 |
| oban | 0 | 1,566 | 56 | 0.0 | 42,283 |
| procrastinate | 160 | 2,508 | 1,487 | 0.0 | 39,480 |
| absurd | 205 | 2,537 | 438 | 0.0 | 16,992 |
| pgmq | 2,907 | 1,834 | 1,451 | 0.0 | 652,926 |
| pgque | 25,178 | 435 | 86 | 0.0 | 1,398 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w16`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 1,263 | 1,162 | 538 | 10.5 | 62 |
| pgboss | 2,048 | 2,111 | 220 | 0.0 | 231,353 |
| river | 317 | 3,808 | 39 | 0.0 | 35,724 |
| oban | 690 | 4,379 | 826 | 0.0 | 90,748 |
| procrastinate | 160 | 2,430 | 1,008 | 0.0 | 18,361 |
| absurd | 173 | 2,433 | 473 | 0.0 | 11,328 |
| pgmq | 6,610 | 1,905 | 462 | 0.0 | 1,073,367 |
| pgque | 8,804 | 438 | 51 | 0.0 | 761 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w4`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 370 | 2,044 | 598 | 12.3 | 59 |
| pgboss | 512 | 1,984 | 83 | 0.0 | 56,713 |
| river | 79 | 3,832 | 39 | 0.0 | 8,916 |
| oban | 226 | 4,206 | 333 | 0.0 | 28,942 |
| procrastinate | 374 | 2,504 | 2,215 | 0.0 | 43,045 |
| absurd | 132 | 2,297 | 358 | 0.0 | 127 |
| pgmq | 3,046 | 1,692 | 240 | 0.0 | 420,082 |
| pgque | 3,060 | 432 | 38 | 0.0 | 422 |

## Depth-target saturation (target depth 4000, 1 ms jobs, one replica) — `sat_w64`

| system | jobs/s | WAL B/job | xacts/s | relfile swaps/s | peak dead tup |
|---|---:|---:|---:|---:|---:|
| awa | 4,614 | 1,111 | 492 | 12.4 | 84 |
| pgboss | 2,478 | 2,387 | 313 | 0.0 | 485,760 |
| river | 1,241 | 4,065 | 51 | 0.0 | 141,699 |
| oban | 1 | 3,121 | 71 | 0.0 | 61,842 |
| procrastinate | 160 | 2,519 | 1,356 | 0.0 | 34,598 |
| absurd | 205 | 2,512 | 517 | 0.0 | 15,055 |
| pgmq | 6,493 | 1,960 | 1,085 | 0.0 | 824,398 |
| pgque | 19,930 | 435 | 75 | 0.0 | 1,153 |

## Reference rate: 800 jobs/s offered, 32 workers

| system | jobs/s | e2e p99 ms | claim p99 ms | producer p99 ms | depth | WAL B/job |
|---|---:|---:|---:|---:|---:|---:|
| awa | 799 | 44.0 | 42.0 | 6.9 | 0 | 1,118 |
| pgboss | 794 | 336.0 | 315.0 | – | 130 | 2,253 |
| river | 627 | – | 45,406.5 | – | 36,360 | 4,554 |
| oban | 800 | – | 161.0 | – | 9 | 4,883 |
| procrastinate | 243 | – | 33,929.8 | – | 10,230 | 2,962 |
| absurd | 191 | 129,085.0 | 129,086.0 | – | 103,334 | 5,825 |
| pgmq | 804 | 22.0 | 12.0 | – | 2 | 1,459 |
| pgque | 796 | 112.8 | 110.9 | 0.3 | 0 | 457 |

## Retry storm: 30% transient / 1% poison failures, max attempts 5

| system | baseline jobs/s | storm good jobs/s | storm claim p99 ms | storm producer p99 ms | storm depth | backlog drain s | latency recovery s | peak dead tup |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| awa | 200 | 138 | 33 | 13 | 0 | 40 | 5 | 5,807 |
| pgboss | 200 | 132 | 8,286 | – | 1,664 | 24 | 115 | 30,144 |
| river | 200 | 138 | 94 | – | 74 | – | 5 | 30,331 |
| oban | 200 | 138 | 58 | – | 2 | 100 | 47 | 35,702 |
| procrastinate | 199 | 136 | 49 | – | 2 | 5 | 207 | 31,881 |
| absurd | 125 | 97 | 107,520 | – | 25,990 | – | – | 7,704 |
| pgmq | 200 | 138 | 33 | – | 2 | 123 | 5 | 27,946 |
| pgque | 200 | 137 | 207 | 5 | 0 | 255 | 118 | 4,987 |

## Queue fan-out: 200 jobs/s total across N queues, then idle — `fanout_q1`

| system | jobs/s | claim p99 ms | backends | LISTEN conns | idle xacts/s | idle PG cores |
|---|---:|---:|---:|---:|---:|---:|
| awa | 200 | 44 | 9 | 2 | 55 | 0.06 |
| pgboss | 200 | 484 | 10 | 0 | 38 | 0.07 |
| river | 200 | 52 | 5 | 0 | 24 | 0.02 |
| oban | 200 | 19 | 301 | 1 | 157 | 0.03 |
| procrastinate | 147 | 965 | 9 | 1 | 292 | 0.08 |
| absurd | 118 | 58,655 | 3 | 0 | 205 | 0.39 |
| pgmq | 201 | 32 | 18 | 0 | 314 | 0.13 |
| pgque | 199 | 121 | 7 | 1 | 40 | 0.04 |

## Queue fan-out: 200 jobs/s total across N queues, then idle — `fanout_q200`

| system | jobs/s | claim p99 ms | backends | LISTEN conns | idle xacts/s | idle PG cores |
|---|---:|---:|---:|---:|---:|---:|
| awa | 200 | 205 | 226 | 2 | 718 | 0.44 |
| pgboss | 200 | 506 | 10 | 0 | 434 | 1.02 |
| river | 200 | 52 | 38 | 0 | 3,631 | 0.72 |
| oban | 200 | 28 | 301 | 1 | 162 | 0.04 |
| procrastinate | 128 | 12,997 | 9 | 1 | 381 | 0.13 |
| absurd | 194 | 13,104 | 202 | 0 | 1,851 | 4.00 |
| pgmq | 200 | 59 | 18 | 0 | 3,312 | 0.87 |
| pgque | 153 | 43,170 | 206 | 200 | 1,658 | 2.09 |

## Logical replication: FOR ALL TABLES publication, streaming then stalled consumer

| system | stream jobs/s | tables w/o RI | lag median B | stall retained WAL B | catalog_xmin age | catalog dead tup | WAL B/job |
|---|---:|---:|---:|---:|---:|---:|---:|
| awa | 399 | 0 | 330,732 | 139,768,784 | 43,938 | 8,126 | 1,389 |
| pgboss | 337 | 0 | 353,876 | 304,317,648 | 70,691 | 148 | 2,541 |
| river | 317 | 0 | 486,152 | 402,451,032 | 29,516 | 214 | 4,823 |
| oban | 400 | 0 | 676,492 | 488,059,176 | 157,118 | 115 | 4,830 |
| procrastinate | 160 | 0 | 171,264 | 151,202,568 | 135,046 | 25 | 3,070 |
| absurd | 168 | 0 | 149,848 | 132,040,456 | 93,320 | 22 | 5,106 |
| pgmq | 402 | 2 | 68,700 | 168,359,512 | 178,512 | 18 | 1,789 |
| pgque | 398 | 5 | 95,408 | 47,503,096 | 21,081 | 114 | 488 |

## Neighbour OLTP: pgbench 300 TPS on the same server (latency ms)

| system | baseline p99 | 1× p99 | 4× p99 | 4× service p99 | 4× tps ratio | 4× queue jobs/s |
|---|---:|---:|---:|---:|---:|---:|
| awa | 5.1 | 20.4 | 68.7 | 28.0 | 1.01 | 1,398 |
| pgboss | 5.2 | 35.3 | 92.0 | 30.5 | 0.99 | 512 |
| river | 5.1 | 22.0 | 55.8 | 26.8 | 1.01 | 317 |
| oban | 5.1 | 64.1 | 167.6 | 42.9 | 1.00 | 579 |
| procrastinate | 5.2 | 21.6 | 15.7 | 14.3 | 1.00 | 238 |
| absurd | 14.8 | 46.3 | 68.8 | 31.9 | 1.00 | 48 |
| pgmq | 5.2 | 11.1 | 76.7 | 27.5 | 0.99 | 1,607 |
| pgque | 5.0 | 5.7 | 6.3 | 5.8 | 1.00 | 1,591 |

## Scheduled herd: 20k jobs due at one instant, immediate stream at 200/s

| system | lateness p50 ms | lateness p99 ms | herd drain s | early starts | immediate claim p99 ms | immediate jobs/s |
|---|---:|---:|---:|---:|---:|---:|
| awa | 7,826 | 15,201 | 15.3 | 0 | 35 | 200 |
| pgboss | 39,080 | 77,284 | 78.1 | 0 | 52,242 | 256 |
| river | 31,588 | 62,542 | 63.2 | 0 | 10,618 | 200 |
| oban | 8,031 | 15,366 | 15.5 | 0 | 28 | 200 |
| procrastinate | 29,113 | 53,997 | 54.5 | 0 | 50,420 | 234 |
| absurd | 189,287 | 238,549 | – | 0 | 127,857 | 93 |
| pgmq | 1,125 | 2,216 | 2.2 | 0 | 36 | 201 |
| pgque | – | – | – | – | 109 | 200 |

## Long jobs: 40 s jobs, 2 × 100 workers, steady state

| system | WAL B/s | xacts/s | tup updated/s | peak dead tup | duplicate completions |
|---|---:|---:|---:|---:|---:|
| awa | 20,546 | 108 | 13.5 | 313 | 0 |
| pgboss | 5,107 | 260 | 4.7 | 286 | 0 |
| river | 11,485 | 52 | 6.1 | 355 | 360 |
| oban | 11,333 | 17 | 5.7 | 387 | 311 |
| procrastinate | 5,955 | 179 | 4.5 | 222 | -0 |
| absurd | 13,836 | 16 | 19.1 | 368 | 1,350 |
| pgmq | 4,597 | 194 | 5.8 | 532 | 610 |
| pgque | 5,784 | 47 | 0.8 | 78 | -842 |

## Backlog drain: 200k jobs preloaded behind a gate, then drained

| system | drain jobs/s | drain s | tail/head rate | DB MB baseline | DB MB peak | DB MB final |
|---|---:|---:|---:|---:|---:|---:|
| awa | 4,786 | 42 | 1.63 | 22.3 | 159.0 | 23.3 |
| pgboss | 2,513 | 72 | 1.00 | 8.3 | 176.7 | 176.7 |
| river | 1,220 | 164 | 1.01 | 7.9 | 217.3 | 217.3 |
| oban | – | – | 2.04 | 7.8 | 244.7 | 244.7 |
| procrastinate | 473 | 220 | 0.98 | 7.8 | 71.5 | 71.5 |
| absurd | – | – | 1.10 | 7.8 | 208.2 | 208.2 |
| pgmq | 13,228 | 15 | 3022.67 | 7.9 | 176.0 | 96.3 |
| pgque | 9,935 | 20 | – | 8.7 | 75.9 | 75.9 |

## Large payloads: 64 KiB incompressible at 50 jobs/s, then drain

| system | jobs/s | WAL B/job | TOAST MB after | DB MB after |
|---|---:|---:|---:|---:|
| awa | 50 | 74,442 | 0.6 | 23.7 |
| pgboss | 50 | 85,220 | 1,173.9 | 1,187.3 |
| river | 50 | 80,938 | 1,174.0 | 1,191.3 |
| oban | 50 | 82,358 | 1,173.8 | 1,191.8 |
| procrastinate | 50 | 81,181 | 1,157.1 | 1,173.6 |
| absurd | 50 | 86,169 | 1,173.9 | 1,193.9 |
| pgmq | 50 | 204,525 | 1,177.9 | 1,188.7 |
| pgque | 50 | 70,705 | 1,176.7 | 1,188.0 |

## Idle background cost (workers running, no jobs)

| system | xacts/s | WAL B/s | relfile swaps/s | PG cores |
|---|---:|---:|---:|---:|
| awa | 335.8 | 352,459 | 21.62 | 0.724 |
| pgboss | 236.7 | 464,446 | 0.00 | 0.377 |
| river | 143.3 | 776,287 | 0.00 | 0.126 |
| oban | 479.2 | 929,005 | 0.00 | 0.215 |
| procrastinate | 1,459.7 | 486,782 | 0.00 | 0.565 |
| absurd | 669.4 | 511,474 | 0.00 | 0.815 |
| pgmq | 564.4 | 289,924 | 0.00 | 0.223 |
| pgque | 103.5 | 105,991 | 0.00 | 0.091 |

## Postgres restart under load (2 replicas, 400 jobs/s)

| system | baseline jobs/s | restart jobs/s | recovery jobs/s | recovery e2e p99 ms | recovery depth |
|---|---:|---:|---:|---:|---:|
| awa | 800 | 294 | 1,038 | 14,242 | 396 |
| pgboss | 678 | 292 | 1,024 | 18,906 | 2,203 |
| river | 634 | 216 | 634 | – | 38,628 |
| oban | 798 | 0 | 0 | – | 9 |
| procrastinate | 667 | 0 | 0 | – | 71,428 |
| absurd | 397 | 0 | 0 | – | 5 |
| pgmq | 402 | 97 | 401 | 15 | 0 |
| pgque | 815 | 404 | 806 | 112 | 0 |

