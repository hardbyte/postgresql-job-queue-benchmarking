# 2026-05-02 — bulk-everywhere worker scaling (alpha.2)

Cross-system throughput at workers ∈ {4, 16, 64, 128}, each adapter
routed through its system's documented bulk producer path. Same
shape as the [2026-05-01 alpha.1
matrix](../2026-05-01-bulk-everywhere/SUMMARY.md); awa upgraded from
0.6.0-alpha.1 to **0.6.0-alpha.2**.

The alpha.2 bump on awa folds in the
[`enqueue_params_copy`](../2026-05-01-bulk-everywhere/SUMMARY.md)
COPY producer (already in alpha.1) plus the [PR
#209](https://github.com/hardbyte/awa/pull/209) `RuntimePayload`
compaction (`skip_serializing_if` on empty `metadata` / `tags` /
`errors` / `progress` — 80-byte default payload → 5 bytes). Other
adapters unchanged.

## Methodology

Identical to the alpha.1 run for direct comparison:

- 30 s warmup + 75 s clean phase per (system, worker_count) cell.
- Producer in **depth-target** mode at `TARGET_DEPTH=2000` with
  `--producer-rate 50000`, `PRODUCER_BATCH_MAX=1000`.
- Same `postgres:17.2-alpine`, single replica, single producer.
- Adapter producer paths unchanged from alpha.1 run.

## Headline

| System | alpha.1 peak | **alpha.2 peak** | Δ vs alpha.1 |
|---|---:|---:|---:|
| **pgque** | 21,790 | **23,295** | +6.9 % |
| **pg-boss** | 4,541 (64w) | **5,787** (64w) | +27 % |
| **awa** | 4,431 | **4,576** | +3.3 % |
| **river** | 2,509 | 2,534 | +1 % |
| **oban** | 1,144 (16w) | 1,192 (16w) | +4 % |
| **procrastinate** | 267 | 268 | flat |

Two-tier picture preserved. Most cell-to-cell deltas are within the
matrix's known run-to-run variance (10–30 % at this scale on
identical code).

- **awa**: +3 % at 128 w. The PR #209 payload-compaction direct A/B
  measured **+14 %** on tight back-to-back alternating runs — that
  lift is real but matrix-level variance washes most of it out, as
  predicted in the alpha.1 SUMMARY's COPY-vs-INSERT note. The shape
  of the curve is unchanged.
- **pgboss** is on its random walk again — 4,051 / 4,971 / 5,768 /
  **5,787** across recent runs at 64 w. Treat as variance, not a
  trend.
- **pgque +7 %** likewise within run-to-run noise.
- **oban 64 / 128 w shutdown-hang** persists (94 / 62 jobs/s) — same
  artefact as the previous two runs. Oban's peak is its 16-w row.

## Per-(system, worker_count) numbers

| System | Workers | Throughput | Producer call p95 | Queue depth |
|---|---:|---:|---:|---:|
| **awa** | 4 | 171 | 18 ms | 3,142 |
| awa | 16 | 676 | 17 ms | 3,588 |
| awa | 64 | 2,376 | 18 ms | 3,912 |
| awa | 128 | **4,576** | 39 ms | 3,812 |
| **pgque** | 4 | 3,357 | 0 ms | 5,000 |
| pgque | 16 | 9,987 | 0 ms | 4,112 |
| pgque | 64 | 19,110 | 0 ms | 2,016 |
| pgque | 128 | **23,295** | 0 ms | 1,734 |
| **pgboss** | 4 | 512 | — | 2,000 |
| pgboss | 16 | 2,048 | — | 1,488 |
| pgboss | 64 | **5,787** | — | 1,296 |
| pgboss | 128 | 4,256 ⚠ | — | 784 |
| **procrastinate** | 4 | 261 | — | 2,602 |
| procrastinate | 16 | 252 | — | 2,406 |
| procrastinate | 64 | 268 | — | 2,415 |
| procrastinate | 128 | 266 | — | 2,353 |
| **river** | 4 | 79 | — | 10,863 |
| river | 16 | 317 | — | 3,239 |
| river | 64 | 1,267 | — | 3,849 |
| river | 128 | **2,534** | — | 7,235 |
| **oban** | 4 | 332 | — | 2,772 |
| oban | 16 | **1,192** | — | 3,155 |
| oban | 64 | 94 ⚠ | — | 21,937 |
| oban | 128 | 62 ⚠ | — | 2,185 |

Caveats unchanged from alpha.1:

- **`oban @ 64 / 128 w`** — same shutdown-hang artefact. Oban's
  peak is its 16-w row.
- **`pgboss @ 128 w`** — below its 64-w number again (same
  inversion as alpha.1's run, persistent enough to be worth a
  closer look in a follow-up).
- **Producer call p95 ≈ 0** for pgque is a sampling resolution
  artefact (single-SQL-call per batch, sub-ms per call).

## Reading

PR #209's payload compaction is the biggest semantic change in this
release and shows up clearly in direct A/B (+14 %) but is within the
matrix's variance envelope (10–30 %) at the matrix level. That's the
honest story: the compaction is a real lift on identical-cell A/B,
but you cannot read it off this table alone. Treat awa @ 128 w at
"steady ~4.5 k jobs/s" — same envelope as alpha.1, no regressions,
modest direct lift on the producer-side allocation path.

The interesting line is still pgque-at-21k+, two orders of magnitude
above procrastinate and roughly 5× awa. That gap is the *consumer
batching* difference, not the producer path — pgque's consumer
amortises across its `read` batch the way none of the other adapters
do.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
for sys in awa pgque procrastinate pgboss river oban; do
  for w in 4 16 64 128; do
    uv run bench run \
      --systems $sys --replicas 1 --worker-count $w \
      --producer-rate 50000 \
      --producer-mode depth-target --target-depth 2000 \
      --phase warmup=warmup:30s --phase clean=clean:75s
  done
done
```

awa-bench routes through `enqueue_params_copy` by default; set
`AWA_QS_PRODUCER_PATH=batch` to A/B against the multi-row INSERT
path.

## Files

- [`matrix.csv`](matrix.csv) — full numerical matrix (24 rows)
- Per-cell raw runs are under `results/custom-20260502T0001*` →
  `results/custom-20260502T0050*` (paths in `matrix.csv`)
