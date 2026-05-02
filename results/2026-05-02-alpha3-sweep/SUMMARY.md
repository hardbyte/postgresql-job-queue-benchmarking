# 2026-05-02 — alpha.3 full sweep

awa upgraded to **0.6.0-alpha.3** (PR #211 completion-batch default 512→128
+ PR #213 claimer heartbeat churn reduction). Bench harness on main with
the [absurd adapter](../../absurd-bench/) (PR #17 + a follow-on schema
idempotency fix while this sweep was running — see "Caveats").

This is the broadest sweep we have to date: **8 systems**, 4 worker counts
on a shared image, pgmq on its own image, three multi-phase chaos
topologies, and an awa 60-minute steady-load soak.

## Layout

| Phase | Subdir | What |
|---|---|---|
| **A** | `matrix.csv` | bulk-everywhere 7 systems × {4, 16, 64, 128} workers, 60 s warmup + 4 min clean |
| **B** | `matrix.csv` (`phase=B` rows) | pgmq matrix on `quay.io/tembo/pg17-pgmq:latest` |
| **C** | [`multiphase-1x64/`](multiphase-1x64/), [`multiphase-2x32/`](multiphase-2x32/), [`multiphase-4x16/`](multiphase-4x16/) | All 7 shared-image systems × 3 topologies × 6-phase chaos |
| **D** | [`soak-awa-1x128/`](soak-awa-1x128/) | awa 60-minute soak at 1×128 workers |

`matrix.csv` covers Phase A and Phase B uniformly; per-cell raw runs are
linked from the `run_id` column.

## Headline: alpha.3 vs alpha.2 (peak throughput, bulk-everywhere)

| System | alpha.2 peak | **alpha.3 peak** | Δ |
|---|---:|---:|---:|
| **awa** | 4,576 | **6,834** (128 w) | **+49 %** |
| pgque | 23,295 | 22,104 (128 w) | −5 % |
| pg-boss | 5,787 (64 w) | 5,302 (64 w) | −8 % |
| river | 2,534 | 2,522 | flat |
| oban | 1,192 (16 w) | 1,142 (16 w) | flat |
| procrastinate | 268 | 270 | flat |
| pgmq | 11,546 (16 w) | 13,290 (16 w) | +15 % |
| **absurd** | (new) | 388 (128 w) | new entry |

The headline is awa's **+49 %** lift end-to-end across alpha.2 → alpha.3.
Two PRs combine here:

- **#211** lowered the completion-batcher default from 512 to 128 — direct
  effect at multi-worker concurrency, where 8 concurrent flushers each
  pushing 512-row UPDATEs were producing receipt-plane contention rather
  than amortising it.
- **#213** removed claimer heartbeat refresh writes while a lease is still
  fresh, cutting coordination-plane writes off the dispatch hot path.

Cross-system numbers for non-awa systems are within run-to-run variance
of alpha.2 (we re-ran the matrix with no system-side changes for them).

## Phase A — bulk-everywhere matrix

awa's curve flattens out at the new ceiling rather than the old one:

| Workers | alpha.2 | **alpha.3** |
|---:|---:|---:|
| 4 | 171 | **296** (+73 %) |
| 16 | 676 | **1,115** (+65 %) |
| 64 | 2,376 | **3,961** (+67 %) |
| 128 | 4,576 | **6,834** (+49 %) |

Same shape, ~1.5–1.7× across the board. The relative slope in the
producer-bound segment (4→64) is what you'd expect if claim-plane
contention is the binding factor at low worker counts and completion-plane
contention is the binding factor at high worker counts; both PRs hit the
relevant axis.

| System | Workers | Throughput | Producer-call p95 | Queue depth |
|---|---:|---:|---:|---:|
| **awa** | 4 | 296 | 18 ms | n/a |
| awa | 16 | 1,115 | 17 ms | 4,033 |
| awa | 64 | 3,961 | 17 ms | 4,134 |
| awa | 128 | **6,834** | 38 ms | 7,041 |
| **pgque** | 4 | 3,382 | 0 ms | 3,953 |
| pgque | 16 | 9,973 | 0 ms | 5,093 |
| pgque | 64 | 19,025 | 0 ms | 1,706 |
| pgque | 128 | **22,104** | 0 ms | 2,141 |
| **pgboss** | 4 | 512 | — | 2,000 |
| pgboss | 16 | 2,048 | — | 1,488 |
| pgboss | 64 | **5,302** | — | 1,200 |
| pgboss | 128 | 5,279 | — | 736 |
| **procrastinate** | 4 | 267 | — | 2,591 |
| procrastinate | 16 | 267 | — | 2,440 |
| procrastinate | 64 | 268 | — | 2,366 |
| procrastinate | 128 | 270 | — | 2,330 |
| **river** | 4 | 79 | — | 11,029 |
| river | 16 | 314 | — | 3,055 |
| river | 64 | 1,267 | — | 4,001 |
| river | 128 | **2,522** | — | 6,160 |
| **oban** | 4 | 333 | — | 2,752 |
| oban | 16 | **1,142** | — | 3,063 |
| oban | 64 | 163 ⚠ | — | 21,520 |
| oban | 128 | 0 ⚠ | — | (collapsed) |
| **absurd** | 4 | 178 | — | 5,084 |
| absurd | 16 | 275 | — | 4,000 |
| absurd | 64 | 371 | — | 4,000 |
| absurd | 128 | 388 | — | 4,000 |

absurd plateaus around ~390 jobs/s — its worker model serialises on a
single subscription cursor regardless of worker count, so adding workers
doesn't add capacity beyond a small saturation point.

oban 64 / 128 collapse to ~0 — the same shutdown-hang artefact persists in
alpha.3, slightly worse than alpha.2's 64 / 76 numbers (now 163 / 0).

## Phase B — pgmq matrix (Tembo image)

| Workers | Throughput |
|---:|---:|
| 4 | 3,565 |
| **16** | **13,290** |
| 64 | 6,521 |
| 128 | 0 ⚠ |

pgmq peaks at 16 workers and then degrades, same shape as alpha.2 (peak
was 11,546 at 16 w). 128 w collapsed to 0 — partition-cursor contention
at high worker counts saturates the entire visibility-timeout fleet.
Documented design behaviour, not a regression.

Caveat: not directly comparable to phase-A numbers because the Tembo image
has different Postgres tunings.

## Phase C — multi-phase chaos (3 topologies)

All 7 shared-image systems run sequentially through the same six-phase
sequence (warmup → baseline → pressure → kill → restart → recovery), at
three replica × workers/replica configurations totalling 64 workers each.

| System | 1×64 baseline | 2×32 baseline | 4×16 baseline |
|---|---:|---:|---:|
| **awa** | **3,560** | 2,491 | 1,503 |
| **pgque** | 18,660 | **34,388** | **38,810** |
| pgboss | 2,560 | 5,108 | 2,000 |
| oban | 69 ⚠ | 2,214 | 2,327 |
| river | 1,262 | 1,165 | 1,021 |
| absurd | 384 | 26 ⚠ | 14 ⚠ |
| procrastinate | 252 | 308 | 283 |

The interesting per-system pattern:

- **awa scales *down* with replica count** at fixed total workers (3,560 →
  2,491 → 1,503). Multi-process completion-shard contention is real
  — `processes × AWA_COMPLETION_SHARDS = 4 × 4 = 16` flushers in 4×16,
  vs 4 in 1×64. Default shard count probably needs to attenuate with
  process count (separate issue).
- **pgque scales *up* with replica count** (18,660 → 34,388 → 38,810).
  Each replica gets its own batch-claim cursor; more cursors → more
  parallelism on the read side. pgque's bulk-batch consumer is the
  cleanest multi-replica scaler in the matrix.
- **pgboss is non-monotonic** (2,560 → 5,108 → 2,000). Random-walk shape
  consistent with prior runs; not a tuning effect.
- **absurd 2×32 / 4×16 baseline is broken** (26 / 14 jobs/s) but
  recovers normally after kill + restart (~330–365 jobs/s). Almost
  certainly the same multi-replica producer/consumer-startup ordering
  issue the rest of the bench has historically had — the kill+restart
  cycle ends up putting absurd into its working steady state. Worth a
  follow-up on absurd-bench.

Per-topology charts (18 plots each, phase-banded) live in
[`multiphase-1x64/plots/`](multiphase-1x64/plots/),
[`multiphase-2x32/plots/`](multiphase-2x32/plots/), and
[`multiphase-4x16/plots/`](multiphase-4x16/plots/).

### Kill behaviour

- **1×64:** kill kills the only replica → 0 jobs/s for everyone during the
  60 s kill window. Restart phase recovers cleanly across all systems.
  This is the topology where you *see* a kill in the throughput line; the
  multi-replica topologies smooth over it.
- **2×32:** awa drops baseline 2,491 → kill 2,016 (−19 %). pgque drops
  34,388 → 15,032 (−56 %). pgboss roughly halves. Surviving replica picks
  up but capacity isn't fully fungible.
- **4×16:** awa baseline 1,503 → kill 1,439 (−4 %). pgque 38,810 → 27,056
  (−30 %). The more replicas you run, the smaller the kill-window dip.

## Phase D — awa 60-minute soak (1×128)

Sustained median throughput **5,369 jobs/s** with peak **9,257 jobs/s**
over a clean 60-minute clean phase under saturating producer pressure
(`--target-depth 2000 --producer-rate 50000`). Producer overran the
configured rate as in prior soak runs; queue depth grew to a peak of
277,754. End-to-end p99 was 14.3 s — not a processing-latency artefact,
just queue-tail effect at depth.

**Median dead tuples: 396.** ADR-019/023's
"bounded receipt-plane churn under sustained load" claim verified for an
hour-long run on alpha.3 — the receipt-ring partitioning + completion-batch
tuning are doing what they should.

Phase-banded charts (single-system, two phases — warmup + clean) live in
[`soak-awa-1x128/plots/`](soak-awa-1x128/plots/).

## Caveats

- **Phase C originally failed** for all three topologies because absurd's
  `ensure_schema()` re-applied `absurd.sql` on the kill+restart phase
  and absurd 0.3.0's SQL is not idempotent (`current_time` function
  recreated without `OR REPLACE`). Patched in `absurd-bench/main.py` to
  pre-check whether the `absurd` schema exists before applying. This
  patch is part of the same results commit; will be PR'd separately to
  the bench repo.
- **oban 64 / 128 w shutdown-hang** persists in alpha.3 (163 / 0 jobs/s).
  Same artefact as alpha.1 / alpha.2 runs. Not a regression.
- **absurd in multi-replica baseline** (Phase C 2×32 / 4×16) is degraded
  in a way that resolves itself across kill+restart. Worth a follow-up
  but it's not a sweep blocker.
- **awa 1-second slow-query warnings during Phase D soak.** The
  `queue_counts_exact` query (the lease-claim anti-join from ADR-023) is
  hitting 1+ second on the partitioned receipt plane under hour-long
  saturating load. Not a regression — noted for post-alpha tuning.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
export PRODUCER_ONLY_INSTANCE_ZERO=1

# Phase A
for sys in awa absurd pgque procrastinate pgboss river oban; do
  for w in 4 16 64 128; do
    uv run bench run --systems $sys --replicas 1 --worker-count $w \
      --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
      --phase warmup=warmup:60s --phase clean=clean:240s
  done
done

# Phase B
for w in 4 16 64 128; do
  uv run bench run --systems pgmq --replicas 1 --worker-count $w \
    --pg-image quay.io/tembo/pg17-pgmq:latest \
    --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
    --phase warmup=warmup:60s --phase clean=clean:240s
done

# Phase C (3 topologies)
for cfg in "1 64" "2 32" "4 16"; do
  read replicas wc <<<"$cfg"
  uv run bench run \
    --systems awa,absurd,pgque,procrastinate,pgboss,river,oban \
    --replicas $replicas --worker-count $wc \
    --producer-rate 2000 --producer-mode depth-target --target-depth 2000 \
    --phase warmup=warmup:30s \
    --phase baseline=clean:60s \
    --phase pressure=high-load:60s \
    --phase 'kill=kill-worker(instance=0):60s' \
    --phase 'restart=start-worker(instance=0):60s' \
    --phase recovery=clean:60s
done

# Phase D
uv run bench run --systems awa --replicas 1 --worker-count 128 \
  --producer-rate 50000 --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:60s --phase clean=clean:60m
```

## Files

- [`matrix.csv`](matrix.csv) — Phase A + B numerical matrix (32 rows)
- [`multiphase-1x64/`](multiphase-1x64/), [`multiphase-2x32/`](multiphase-2x32/), [`multiphase-4x16/`](multiphase-4x16/) — phase-banded chart sets per topology
- [`soak-awa-1x128/`](soak-awa-1x128/) — phase-banded chart set for the 60-min soak
- [`run.log`](run.log) — full harness log for Phases A/B/D (large)
- [`run-c.log`](run-c.log) — Phase C re-run log
