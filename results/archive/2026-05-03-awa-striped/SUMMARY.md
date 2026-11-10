# 2026-05-03 — awa striped queue-storage matrix

awa-only, single very hot queue, sweeping `queue_storage_queue_stripe_count
∈ {1, 2, 4}` × `worker_count ∈ {64, 128, 256, 512}`. Tests what striping
the physical queue buys at high concurrency, now that
[PR #215](https://github.com/hardbyte/awa/pull/215) makes multi-stripe
runtime claims non-deadlocking.

awa: post-#215 main (commit `d930e72`, `v0.6.0-alpha.3-4-gd930e72`).

## Methodology

- 60 s warmup + 4 min clean phase per cell.
- Producer in **depth-target** mode at `TARGET_DEPTH=4000` with
  `--producer-rate 50000`, `PRODUCER_BATCH_MAX=1000`. Same shape as
  the bench numbers in PR #215; deeper than the alpha.3 cross-system
  sweep (which ran `target-depth=2000`).
- 1 replica × N workers per cell.
- Stripe count varied via `QUEUE_STRIPE_COUNT` env var on the
  awa-bench adapter.

## Headline

| stripes | 64 w | 128 w | 256 w | 512 w |
|--:|--:|--:|--:|--:|
| **1** | 3,408 | 4,741 | 9,530 | 11,188 |
| **2** | **4,654** (+37 %) | **7,667** (+62 %) | **11,975** (+26 %) | 11,173 |
| **4** | 4,378 (+28 %) | 7,596 (+60 %) | 11,418 (+20 %) | 11,443 (+2 %) |

Throughput percentages are vs `stripes=1` at the same worker count.

## End-to-end p99 latency (ms, median across the clean phase)

| stripes | 64 w | 128 w | 256 w | 512 w |
|--:|--:|--:|--:|--:|
| **1** | 3,605 | 2,761 | 1,802 | 1,600 |
| **2** | 3,553 | **2,036** (−26 %) | **1,027** (−43 %) | 1,405 (−12 %) |
| **4** | 3,693 | 2,103 (−24 %) | 1,037 (−42 %) | **1,057** (−34 %) |

## Reading

- **Stripe count = 2 is the clear sweet spot** for this workload. At
  every worker count from 64 through 256 it strictly dominates 1
  stripe on both throughput and tail latency. The largest absolute win
  is at 256 workers: **11,975 jobs/s** at p99 1,027 ms versus 1-stripe's
  9,530 jobs/s at p99 1,802 ms — +26 % throughput while *halving* the
  p99 latency.
- **Stripe count = 4 ≈ stripe count = 2** within run-to-run variance
  for most cells. The 1→2 lift is real and large; the 2→4 step adds
  no meaningful capacity at this offered load. The one cell where 4
  edges 2 is 512 w (1,057 vs 1,405 ms p99) — at very high concurrency
  the extra physical stripe smooths a tail spike, but the throughput
  is the same.
- **Throughput plateaus around ~11–12 k jobs/s by 256 workers.** Past
  that, all three stripe counts converge on a shared ceiling — at this
  configuration we're hitting something other than claim-side stripe
  contention (probably the producer-call path or replica-side worker
  pool overhead). The latency-only improvement at 512 w from
  stripes=4 is consistent with that read.
- **The 1-stripe → 2-stripe lift is biggest in the middle of the
  worker range.** +62 % at 128 w is the headline percentage. At low w
  there isn't enough offered concurrency to surface stripe contention;
  at high w some other bottleneck binds.

## Recommendation

For the "single very hot queue, many workers" scenario this run
exercises, **`queue_storage_queue_stripe_count=2` is the right
default**. It delivers a substantial throughput lift across the
worker range, halves the p99 latency at the load where the
cross-system bench peaks (around 128–256 workers), and there's no
penalty against `stripes=1` at any cell measured here.

`stripes=4` is reasonable under heavier load (512 w) where it holds
tail latency better, but for the typical deployment shape `stripes=2`
captures most of the win at lower coordination cost. Higher values
should still be benchmarked against the specific workload before use,
per the configuration docs.

## Why this isn't in the cross-system matrix

The cross-system bench at `results/2026-05-02-alpha3-sweep/`
intentionally runs awa with `stripes=1` (the default) so the headline
numbers reflect what users get out of the box, not a tuned config
unavailable to other systems. This artifact lives separately to
document the within-awa tuning story.

## Caveats

- Single replica. Multi-replica with stripes is a separate story —
  PR #215's fix is exactly about non-deadlocking concurrent
  claim+enqueue across stripes, and the regression test exercises it
  but the cross-replica throughput shape isn't surveyed here.
- `target-depth=4000` matches PR #215's bench shape; `target-depth=2000`
  (used in the alpha.3 cross-system sweep) would shift the absolute
  numbers slightly. The relative deltas are the load-bearing finding.
- Run-to-run variance at 128+ w is in the 5–10 % band on this
  hardware, so single-cell deltas under 10 % should be read as noise.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
for stripes in 1 2 4; do
  for w in 64 128 256 512; do
    QUEUE_STRIPE_COUNT=$stripes \
    uv run bench run \
      --systems awa --replicas 1 --worker-count $w \
      --producer-rate 50000 \
      --producer-mode depth-target --target-depth 4000 \
      --phase warmup=warmup:60s --phase clean=clean:240s
  done
done
```

## Files

- [`matrix.csv`](matrix.csv) — full numerical matrix (12 rows)
- [`run.log`](run.log) — harness log
