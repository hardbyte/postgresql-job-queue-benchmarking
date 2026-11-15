# Operator scenarios, November 2026

Two scenarios cover operator work a production queue has to get right. They ran on the same host and PostgreSQL 18.6 (4 CPUs) as the November comparison, one system at a time, on 15–16 November 2026 with harness `brian/operator-scenarios`. awa ran twice: the 0.6 benchmark pin (`release/0.6.6` at `1a53fd9e`), and unreleased `main` at `53e1fc97` (0.7.0-alpha.2) from a repinned checkout (`results/2026-11-16-operator-awa07`). The adapter contract is in `docs/operator-scenarios.md`.

- **Find a job.** A million jobs are queued in a queue no worker consumes. Each has a unique handle and one of 1,000 tenants, and every 20th has a second kind. Meanwhile 200 jobs/s of ordinary work runs on the bench queue. Probes run at 2/s for 3 minutes on their own connections, through each system's API (SQL where that is the interface), and are checked against the known answer. A probe is capped at 30 s.
- **Pause and resume.** Two processes run at 200 jobs/s each. The bench queue is paused, process 1 is restarted while paused, then the queue is resumed. Systems with no pause stop each process's workers locally ("emulated"); a restarted process doesn't know about that stop.

## Findings

- **Pause:** a pause survives a restart only where the system stores it. awa, River and PgQue started no job while paused, including on the restarted process. Oban open source holds the pause in memory, so its restarted node started 21,435 jobs. The four systems without a pause API (Absurd, pg-boss, Procrastinate, pgmq) can only stop each process.
- **Unique field:** River finds one job by a unique field in 2.4 ms (GIN on metadata). Absurd, pgmq, Oban and pg-boss take 70–190 ms, and their plans scan. awa 0.6's `list_jobs` checks tags after loading the jobs, so every tag probe hit the 30 s cap. Procrastinate and PgQue can't filter on job data at all.
- **Procrastinate's ordinary queue stalled behind the haystack.** Its bench queue fell to 10–18 jobs/s, with a pickup p99 of 91–200 s, because the worker's fetch scans past todo jobs in other queues.
- **awa 0.7 (unreleased)** answers every probe, including the tag filters that time out on 0.6, but every probe takes 0.5–1.2 s, lookup by id included. Reproduced on an isolated cluster, with ANALYZE not helping. There are two causes:
  - `get_job`/`find_jobs` read terminal jobs through a receipt-batch probe that now matches `job_id = ANY($1)` instead of `= $1`, so it hash-joins every `ready_entries` row and spills to temp files.
  - `queue_overview` lacks 0.6's per-lane exact-count cap and walks every available row.
  Both are being fixed before release.

### Finding one job among a million

p50 / p99 per probe, with how it was answered. Live load = claim p99 on the bench queue before and while probing.

| System | Haystack load | Get by id | Unique field | Tenant page | Kind + state page | Count | Live load (claim p99) |
|---|---:|---|---|---|---|---|---|
| absurd | 134 s | 0.8 ms / 1.2 ms | 72.6 ms / 79.7 ms (SQL) | 76.5 ms / 92.6 ms (SQL) | 107 ms / 126 ms (SQL) | 51.4 ms / 57.8 ms (SQL) | 63.0 ms → 17.0 ms |
| awa | 35 s | 9.8 ms / 103 ms | 30,001 ms / 30,001 ms (timed out) | 30,001 ms / 30,001 ms (timed out) | 1,413 ms / 2,405 ms | 56.3 ms / 149 ms | 44.5 ms → 13.0 ms |
| awa 0.7 | 47 s | 471 ms / 551 ms | 510 ms / 588 ms | 533 ms / 665 ms | 721 ms / 903 ms | 1,038 ms / 1,185 ms | 29.0 ms → 19.0 ms |
| oban | 73 s | 0.4 ms / 1.1 ms | 188 ms / 206 ms | 14.6 ms / 28.4 ms | 3.0 ms / 4.1 ms | 120 ms / 147 ms | 14.0 ms → 13.0 ms |
| pgboss | 50 s | 1.0 ms / 2.0 ms | 192 ms / 205 ms | 65.0 ms / 70.0 ms | 2.0 ms / 3.0 ms | 1.0 ms / 1.0 ms | 474 ms → 236 ms |
| pgmq | 13 s | 0.3 ms / 0.9 ms (SQL) | 144 ms / 164 ms (SQL) | 20.3 ms / 60.1 ms (SQL) | 0.9 ms / 1.9 ms (SQL) | 38.3 ms / 48.2 ms | 46.0 ms → 43.0 ms |
| pgque | 12 s | unsupported | unsupported | unsupported | unsupported | 0.5 ms / 0.9 ms | 107 ms → 107 ms |
| procrastinate | 91 s | 1.0 ms / 1.7 ms | unsupported | unsupported | 405 ms / 433 ms | 431 ms / 655 ms | 90,864 ms → 200,306 ms |
| river | 30 s | 0.3 ms / 0.8 ms | 2.4 ms / 290 ms | 13.1 ms / 29.3 ms | 2.8 ms / 4.0 ms | 125 ms / 138 ms (SQL) | 112 ms → 52.0 ms |

### Pausing and resuming a queue

| System | Pause | Jobs started while paused | Last start after pause | First start after resume | Back to 90% |
|---|---|---:|---:|---:|---:|
| absurd | emulated | 11,584 | 55,480 ms | 23.0 ms | 0.4 s |
| awa | API | 0 | 0.0 ms | 28.0 ms | 0.6 s |
| awa 0.7 | API | 0 | 0.0 ms | 37.0 ms | 1.3 s |
| oban | API | 21,435 | 58,015 ms | 16.0 ms | 0.2 s |
| pgboss | emulated | 7,680 | 54,882 ms | 3.0 ms | 0.7 s |
| pgmq | emulated | 21,014 | 55,492 ms | 53.0 ms | 0.2 s |
| pgque | API | 0 | 0.0 ms | 153 ms | 1.0 s |
| procrastinate | emulated | 16,203 | 55,008 ms | 8.0 ms | 0.1 s |
| river | API | 0 | 0.0 ms | 28.0 ms | 0.8 s |
