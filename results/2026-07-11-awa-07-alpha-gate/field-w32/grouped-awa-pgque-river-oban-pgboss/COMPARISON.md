# Cross-System Comparison

Generated from `custom-20260711T012219Z-ea357e`. Phase: `clean`.

Numbers are **medians across the clean phase**. The interactive cross-system report (`index.html`) carries the full timeline and per-replica overlays. Raw samples are in `raw.csv`.

**Phase shape:** `clean` ran for 300s (5 minutes) after a 5-minute warmup that's excluded from these aggregates.

**Workload:** 200 jobs/s offered load, 8 worker concurrency, 1 replica per system. Same Postgres image, same scenario, same producer.

## Headline metrics

| Metric | awa | pgque | river | oban | pgboss |
|---|---|---|---|---|---|
| Throughput (jobs/s, sustained) | 800.0 | 801.1 | 633.6 | 800.2 | 753.6 |
| Enqueue rate (jobs/s offered) | 800.1 | 800.9 | 800.0 | 799.9 | 754.1 |
| End-to-end latency p50 (ms) | 12.007 | 65.133 | — | — | 215.0 |
| End-to-end latency p95 (ms) | 17.007 | 111.5 | — | — | 392.5 |
| End-to-end latency p99 (ms) | 32.015 | 115.9 | — | — | 416.5 |
| Claim latency p95 (ms) | 15.007 | 110.1 | 37,667.5 | 30.500 | 375.5 |
| Producer latency p95 (ms) | 0.178 | 0.197 | — | — | 3.000 |
| Producer call latency p95 (ms) | 3.578 | 0.197 | — | — | — |

A dash (`—`) means the adapter doesn't sample that metric. Adapter metric sets vary; the awa adapter samples producer / claim / end-to-end latency, while peer adapters often sample only their native equivalent. See each adapter's `main.py` / `bench.rs` for the exact sample set.

## Dead tuples — totals across queue-storage / adapter tables

Sum of `n_dead_tup@*` across every sampled table. Median and peak are taken across the clean phase; lower is better. Per-table breakdown lives in `summary.json` under each system's metrics block.

| System | Median | Peak |
|---|---:|---:|
| awa | 54 | 79 |
| pgque | 572 | 1,097 |
| river | 37,572 | 72,649 |
| oban | 50,402 | 98,303 |
| pgboss | 42,990 | 87,358 |

## Caveats

- `awa-canonical` is the same Rust binary as `awa` forced onto the pre-0.6 storage path. Useful as the within-codebase before/after; not an independent system.
- `awa-python` runs the same Rust core via PyO3 — differences vs. `awa` reflect the FFI overhead, not the storage engine. The Python-side bench harness samples a smaller metric set than the Rust adapter, so several latency rows show as `—`.
- River is a Go job framework, not Postgres-native — its claim path uses different SQL and its lifecycle is framework-shaped. Compare on what each system promises, not on apples-to-apples lifecycle.
- Dead tuples are heavily affected by autovacuum cadence (`autovacuum_naptime=60s` here) and per-table thresholds. Lower median is good; a large peak with a stable median means vacuum is keeping up — that's not a regression.
- Throughput should track offered load (200/s). A system completing well below that means the worker pool is the bottleneck, not the queue engine.
