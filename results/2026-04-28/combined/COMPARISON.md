# Cross-System Comparison

Generated from `custom-20260428T122725Z-545a10`. Phase: `clean_1`.

Numbers are **medians across the clean phase**. The interactive cross-system report (`index.html`) carries the full timeline and per-replica overlays. Raw samples are in `raw.csv`.

**Phase shape:** `clean_1` ran for 6900s (115 minutes) after a 5-minute warmup that's excluded from these aggregates.

**Workload:** 200 jobs/s offered load, 8 worker concurrency, 1 replica per system. Same Postgres image, same scenario, same producer.

## Headline metrics

| Metric | awa | awa-canonical | procrastinate | river | oban | pgque | pgboss |
|---|---|---|---|---|---|---|---|
| Throughput (jobs/s, sustained) | 200.0 | 195.4 | 196.7 | 134.0 | 111.8 | 192.8 | 200.0 |
| Enqueue rate (jobs/s offered) | 200.0 | 195.0 | 196.7 | 133.8 | 111.8 | 193.6 | 200.2 |
| End-to-end latency p50 (ms) | 11.007 | 33.023 | — | — | — | 67.372 | 151.0 |
| End-to-end latency p95 (ms) | 15.007 | 59.007 | — | — | — | 114.9 | 342.0 |
| End-to-end latency p99 (ms) | 29.007 | 102.0 | — | — | — | 152.6 | 375.0 |
| Claim latency p95 (ms) | 13.007 | 57.023 | 10.719 | 52.000 | 15.000 | 113.3 | 325.0 |
| Producer latency p95 (ms) | 0.869 | 1.077 | — | — | — | 4.865 | 4.000 |
| Producer call latency p95 (ms) | 4.343 | 5.387 | — | — | — | 4.865 | — |

A dash (`—`) means the adapter doesn't sample that metric. Adapter metric sets vary; the awa adapter samples producer / claim / end-to-end latency, while peer adapters often sample only their native equivalent. See each adapter's `main.py` / `bench.rs` for the exact sample set.

## Dead tuples — totals across queue-storage / adapter tables

Sum of `n_dead_tup@*` across every sampled table. Median and peak are taken across the clean phase; lower is better. Per-table breakdown lives in `summary.json` under each system's metrics block.

| System | Median | Peak |
|---|---:|---:|
| awa | 225 | 579 |
| awa-canonical | 64,213 | 272,990 |
| procrastinate | 61,471 | 269,995 |
| river | 42,738 | 194,609 |
| oban | 33,660 | 157,065 |
| pgque | 576 | 1,119 |
| pgboss | 64,990 | 265,330 |

## Caveats

- `awa-canonical` is the same Rust binary as `awa` forced onto the pre-0.6 storage path. Useful as the within-codebase before/after; not an independent system.
- `awa-python` runs the same Rust core via PyO3 — differences vs. `awa` reflect the FFI overhead, not the storage engine. The Python-side bench harness samples a smaller metric set than the Rust adapter, so several latency rows show as `—`.
- River is a Go job framework, not Postgres-native — its claim path uses different SQL and its lifecycle is framework-shaped. Compare on what each system promises, not on apples-to-apples lifecycle.
- Dead tuples are heavily affected by autovacuum cadence (`autovacuum_naptime=60s` here) and per-table thresholds. Lower median is good; a large peak with a stable median means vacuum is keeping up — that's not a regression.
- Throughput should track offered load (200/s). A system completing well below that means the worker pool is the bottleneck, not the queue engine.
