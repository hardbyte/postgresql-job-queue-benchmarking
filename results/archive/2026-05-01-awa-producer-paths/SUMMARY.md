# 2026-05-01 — awa producer-path optimisations

awa shipped two producer-side changes in 0.6.0-alpha.1
([hardbyte/awa#206](https://github.com/hardbyte/awa/pull/206) +
[#208](https://github.com/hardbyte/awa/pull/208)):

1. **`enqueue_params_copy`** — direct `COPY ... FROM STDIN` into
   `ready_entries` and `deferred_jobs`, alongside the existing
   `enqueue_params_batch` (multi-row INSERT).
2. **Batched uniqueness-claim sync** — for jobs with `unique_opts`,
   one array-`unnest` INSERT instead of one `INSERT` per row.

Both kick in on the queue-storage producer path; consumer-side
behaviour is unchanged. Numbers from local A/B runs.

## Sustained throughput on the bench scenario

![awa producer paths](plots/awa_producer_paths_throughput.png)

| Workers | INSERT (batch) | COPY | Lift |
|---:|---:|---:|---:|
| 64 | 2,361 jobs/s | 2,534 jobs/s | +7.3 % |
| 128 | 4,249 jobs/s | 4,643 jobs/s | +9.3 % |

Same shape as the
[bulk-everywhere matrix](../2026-05-01-bulk-everywhere/SUMMARY.md):
depth-target producer at `TARGET_DEPTH=2000`, `PRODUCER_BATCH_MAX=1000`,
75 s clean phase, single replica, single `postgres:17.2-alpine`. The
producer-call p95 — the per-batch insert latency where COPY is
expected to win most directly — drops 18 % at 64 workers and 7 % at
128 workers.

These numbers track the prior in
[awa#207](https://github.com/hardbyte/awa/issues/207): at high worker
count the consumer is WAL-bound on completion writes, so a faster
producer can offer more pressure but workers can only consume what
they consume. COPY trims a real component of per-job cost; it doesn't
change the fundamental fsync rate × bytes-per-row ceiling.

## Unique-heavy enqueue (PR #208 specifically)

![awa unique-heavy enqueue](plots/awa_unique_heavy_enqueue.png)

The PR's included benchmark on this rig:

| | INSERT (batch) | COPY |
|---|---:|---:|
| 8,192 unique jobs, batch_size=128 | 23,326 jobs/sec | 25,647 jobs/sec |

This is a different scenario from the bench matrix — local Postgres,
one-shot enqueue, all jobs carry `unique_opts`. Pre-PR the per-row
`sync_unique_claim` × N rounds dominated; post-PR it's a single
array-INSERT. Order-of-magnitude lift on the unique-heavy path; the
INSERT-vs-COPY ordering at the top of that envelope is
hardware-dependent (PR description had COPY at 0.92×, this rig has
1.10×, the difference is fsync rate × parse-cost balance — both
paths scale fine at this batch size).

## What this means for the cross-system comparison

The bulk-everywhere matrix already reports awa at 4,566 jobs/s @ 128
workers. With `enqueue_params_copy` the same scenario lands ~9 %
higher; the next time the matrix runs against awa 0.6.0-alpha.1 the
awa numbers will tick up accordingly. The headline two-tier picture
(awa / pg-boss climbing with workers; pgque higher in absolute terms;
oban / river / procrastinate plateauing on consumer side) is
unchanged.

The bigger-picture finding from
[awa#207](https://github.com/hardbyte/awa/issues/207) holds: at the
worker counts where pgque sustains 22 k jobs/s, awa's ceiling lives
in **per-job WAL bytes**, not the producer API. Future awa-side
work in that direction (smaller default payload, fewer per-row
metadata columns) would compound; producer-API work has now mostly
happened.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
# Build awa-bench against awa 0.6.0-alpha.1; flip the env to the
# COPY path:
AWA_QS_PRODUCER_PATH=copy uv run python long_horizon.py run \
  --systems awa --replicas 1 --worker-count 128 \
  --producer-rate 50000 \
  --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s --phase clean=clean:75s
```

Unique-heavy benchmark (run from a checkout of `hardbyte/awa` on
0.6.0-alpha.1):

```sh
DATABASE_URL=postgres://postgres:test@localhost:15432/awa_test \
AWA_QS_COPY_BENCH_JOBS=8192 AWA_QS_COPY_BENCH_BATCH=128 \
cargo test --release -p awa-model --test queue_storage_copy_test \
  queue_storage_copy_benchmark_unique_batch_vs_copy -- --ignored --nocapture
```

## Files

- [`plots/awa_producer_paths_throughput.png`](plots/awa_producer_paths_throughput.png)
- [`plots/awa_unique_heavy_enqueue.png`](plots/awa_unique_heavy_enqueue.png)
