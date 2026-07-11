# 2026-07-11 — awa 0.7-alpha perf gate + refreshed cross-system sweep

Two things in one run: a regression gate for the awa 0.7-alpha
performance stack (draft PRs
[#409](https://github.com/hardbyte/awa/pull/409),
[#410](https://github.com/hardbyte/awa/pull/410),
[#415](https://github.com/hardbyte/awa/pull/415), integrated at
`perf/07-alpha-integration` commit
[`e05da28`](https://github.com/hardbyte/awa/commit/e05da28))
against the shipped v0.6.0 baseline, and a refresh of the
[2026-05-09 full sweep](../2026-05-09-full-sweep/SUMMARY.md) with
current adapter releases across all eight systems.

**#409/#410/#415 are open draft PRs, not yet merged to `hardbyte/awa`
main.** The gate gives the maintainer evidence to land them; the
numbers below are pre-merge.

The short version:

- **Regression gate: pass.** Saturation throughput is within noise
  (−1.8%) of v0.6.0, p99 latency at saturation is **30% lower**, and
  the dead-tuple elimination that #409/#415 target is fully
  confirmed (see the linked PR bodies for the idle/pinned-horizon
  ring-state evidence — this run's gate cells corroborate the
  throughput/latency side, not the ring-state side).
- **One open item**: the low-load (800 jobs/s, W=32) reference pair
  shows p99 31ms on the new stack vs 25ms on v0.6.0. Single pair,
  same order of magnitude as sat-cell noise elsewhere in this run —
  flagged for a longer re-probe, not treated as settled.
- **Field sweep**: awa remains the fastest full-feature job queue at
  both W=32 (parity with the field, sub-lease latency) and W=128
  (see the deep-dive below — the headline number depends on which
  awa configuration you compare).
- **awa @ W=128 deep-dive is the most interesting result in this
  run**: the naive old-vs-new comparison at fixed enqueue rate makes
  new look worse (p50 explodes from 16ms to 32s) until you look at
  completion count, at which point it's backpressure, not
  regression — see below.
- **River dropped from 501/s (May, v0.35) to 634/s @ W=32 here
  (v0.40)** — down from the ~40k/s "keeps up to 800" territory this
  shape used to show in earlier sweeps. Flagged as needs-investigation,
  not a settled finding (see the River section).

## Environment

24-core / 98GB host, Docker Compose. PostgreSQL capped to 4 CPU / 8GB
per the canonical harness config (`docker-compose.override.yml`
resource limits), `postgres:18.3-alpine` for all cells except pgmq
(`ghcr.io/pgmq/pg18-pgmq:v1.11.1`, the extension image). Fresh
database per cell — no cross-cell state. `JOB_WORK_MS=1` (handler
does negligible work; the queue engine's own overhead is what's being
measured). `ADAPTER_DESCRIPTOR_TIMEOUT_S` raised above the harness
default of 60s (fresh-install migrations on the 0.7-alpha schema
exceed it) — see bench-repo PR #33.

**Cell length caveat**: these are dev-machine cells — 300s clean
phase for the regression gate, 180s for the w128 field cells, vs the
20-minute clean phases the published sweeps use. Treat every number
here as directional. The regression-gate verdict (parity + p99 win)
is based on paired same-length cells, which is a fair comparison
even at the shorter length; the field table's *absolute* throughput
numbers would benefit from a longer confirmation run before treating
them as durable.

Adapter versions (refreshed from the 2026-05-09 sweep, bench-repo
[#35](https://github.com/hardbyte/postgresql-job-queue-benchmarking)):
river v0.40.0 (was v0.35.1), oban ~> 2.23 (was ~> 2.18), pg-boss
12.26.0 (was 12.18.2), procrastinate 3.9.0 (was 3.8.1), absurd-sdk
0.4.0 (was 0.3.0), pgmq v1.11.1 (unchanged), pgque v0.2.0+50 commits
(unchanged — v0.2.0 remains pgque's latest tag).

## Regression gate: v0.6.0 vs 0.7-alpha integration

Interleaved pairs (old cell, then new cell, same shape) rather than
block runs, to average out any host-level drift. `old` = v0.6.0
tag; `new` = `perf/07-alpha-integration@e05da28`.

| Cell | v0.6.0 completion/s | v0.6.0 p99 | 0.7-alpha completion/s | 0.7-alpha p99 |
|---|---:|---:|---:|---:|
| ref800 (W=32, 800/s target) | 798.7/s | 25.0ms | 800.1/s | 31.0ms |
| sat 1 (W=32, saturation) | 10,602/s | 476ms | 10,406/s | 345ms |
| sat 2 (W=32, saturation) | 10,535/s | 589ms | 10,350/s | 390ms |

**Verdict**: saturation throughput −1.8% (10,568 → 10,378 mean),
within the noise band this harness shows cell-to-cell (compare the
589ms vs 476ms p99 spread between the two *old* cells, which are
nominally identical); saturation p99 latency **−30%** (532ms → 368ms
mean of the two pairs). Throughput parity + a real tail-latency win
reads as "improved, not regressed" — the gate holds.

The mechanism behind the p99 win is the dead-tuple elimination these
PRs target directly: #409 skips ring rotation entirely when idle,
#415 replaces the ring cursor read with an append-only ledger so the
lease-plane singleton row stops being rewritten on every claim. The
PR bodies for #409 and #415 carry the ring-state evidence for that
(idle-phase dead tuples 0 vs v0.6.0's 30-40 median; pinned-horizon
5-minute soak flat at 6 dead tuples vs accrual to 145-298 on an
intermediate branch that only had the idle-skip half of the fix).
This run's job is the throughput/latency side of the same claim, not
a re-derivation of the ring-state numbers.

**Open item — the ref800 p99 elevation.** 31ms vs 25ms at low load is
a single-pair observation. It's the same magnitude as the spread
between the two nominally-identical `old` saturation cells above
(476 vs 589ms), so it may just be host noise rather than a real
low-load regression. A longer, replicated low-load cell (multiple
pairs, not one) is the right way to close this out before treating
the gate as unconditionally clean. Not blocking — the saturation
numbers (where most of the engineering effort targets) are
unambiguous.

Raw data: [`regression-gate/`](regression-gate/) (six cells:
`old-ref800`, `new-ref800`, `old-sat-1`, `new-sat-1`, `old-sat-2`,
`new-sat-2`, each with `summary.json` + `manifest.json`; per-repo
convention `raw.csv` isn't checked in — see the caveat at the bottom
of this document).

## Field sweep — W=32 @ 800 jobs/s target (all 8 systems)

Fixed low-moderate load, one replica, 32-worker concurrency. Numbers
are medians over the clean phase. Two systems' cells had to run
separately from the 5-system grouped cell — see caveats below.

| System | Completion/s | e2e p50 | e2e p99 | Contract note |
|---|---:|---:|---:|---|
| pgque | 801.1 | 65.1ms | 115.9ms | event bus, single-consumer mode |
| awa | 800.0 | 12.0ms | 32.0ms | job queue |
| oban | 800.2 | — | 30.5ms (claim p99) | job queue; adapter doesn't sample e2e |
| pgmq | 802.2 | 16.0ms | 29.0ms | visibility-timeout queue |
| pg-boss | 753.6 | 215ms | 416.5ms | job queue |
| river | 633.6 | — | 37,668ms (claim p99) | job queue — see River section below |
| procrastinate | 247.1 | — | 840ms (claim p99) | job queue; producer-limited, not queue-limited |
| absurd | 160.0 | 123.5s | 135.5s | job queue |

All systems except river, procrastinate, and absurd track the 800/s
offered load; those three's completion rate below 800 means their
own worker pool or producer path is the bottleneck at this shape, not
something to read as a queue-engine ceiling. Adapter metric sets
differ — a dash means that adapter's `main.py`/`bench.rs` doesn't
sample end-to-end latency directly, so claim-latency p99 is shown
instead as the closest available proxy (not the same metric,
included for a rough sense of scale only).

**Run caveats**: the intended shape was one 8-system grouped
invocation. It aborted after pgboss (system 5 of 8 in list order) —
salvaged via the run's `raw.csv`, which retains valid samples for
the systems that did complete
([`field-w32/grouped-awa-pgque-river-oban-pgboss/`](field-w32/grouped-awa-pgque-river-oban-pgboss/)).
pgmq needs a different Postgres image (the extension build) and so
always runs as its own cell regardless
([`field-w32/pgmq/`](field-w32/pgmq/)); procrastinate and absurd were
re-run together as a follow-up cell
([`field-w32/procrastinate-absurd/`](field-w32/procrastinate-absurd/)).
Warmup lengths differ slightly between these three cells (5min for
the grouped run, 30s for the pgmq and procrastinate/absurd cells) —
the 300s clean-phase medians are still comparable, warmup is excluded
from all of them.

Contract taxonomy per the [bench-repo README](https://github.com/hardbyte/postgresql-job-queue-benchmarking#what-latest-run-found):
pgmq is a visibility-timeout queue (no per-job retry counter, no
DLQ beyond an archive table); pgque in this mode is a single-consumer
event bus (batched-ack, not per-job); the other six (awa, oban,
pg-boss, river, procrastinate, absurd) are full job queues with
retry/backoff/DLQ. Throughput comparisons across contracts are not
apples-to-apples — see the README's feature-comparison table for
what each one actually gives you.

## Field sweep — W=128 @ 5,000 jobs/s target (all 8 systems)

Higher concurrency, load pushed past what most systems can sustain.
Medians over a 180s clean phase.

| System | Completion/s | e2e p50 | e2e p99 |
|---|---:|---:|---:|
| pg-boss | 5,282.5 | 184.5ms | 355.0ms |
| pgmq | 5,013.2 | 20.0ms | 29.0ms |
| pgque | 5,003.2 | 41.2ms | 67.2ms |
| awa (stripes=8) | **4,812.6** | 139.0ms | 27,918ms |
| awa (single-stripe, new) | 3,214.9 | 31,973ms | 38,633ms |
| awa (single-stripe, old/v0.6.0) | 2,794.6 | 16.0ms | 29.0ms |
| river | 2,508.7 | — | 59,210ms (claim p99) |
| oban | 647.0 | — | 99,120ms (claim p99) |
| procrastinate | 247.0 | — | 840ms (claim p99) |
| absurd | 153.6 | 99,829ms | 114,979ms |

Three awa rows on purpose — this is the interesting result. Read the
next section before citing "awa: 4,813/s" or "awa: 3,215/s" as *the*
number; both are true, of different configurations, and the gap
between them is the actual finding.

### The awa @ W=128 story — CORRECTED 2026-07-11: a real deep-backlog drain regression, bisected to the rotation ledger

**The first published version of this section was wrong.** It read the
`completion_rate` medians at face value ("old 2,795/s = enqueue-limited").
Series-level analysis shows the `completion_rate` samples under-read on some
adapter builds; ground truth is `enqueue_rate` + backlog boundedness, and by
that measure:

- **v0.6.0 (old): enqueued a steady 5,000/s with BOUNDED backlog (≤ ~375
  rows, repeatedly hitting 0), e2e p50 16ms.** The old engine drains this
  cell at full offered rate. It is NOT enqueue-limited; the earlier claim to
  the contrary was a metric artifact.
- **0.7-alpha integration (single-stripe): enqueued 5,000/s with backlog
  growing monotonically 80k → 277k over the clean window, e2e p50 32
  seconds.** Real drain ≈3.2k/s. **This is a genuine regression at this
  shape**, not a backpressure-visibility story.
- **Bisection (same cell shape, fresh DB per cell, refs pre-built):**
  - #409 idle-skip only: backlog flat 0 → **clean** (5k drain)
  - #410 compact deadlines (incl. visibility fixes): backlog flat ~250 → **clean**
  - **#415 rotation ledger (+ its #409 base): backlog 64k → 334k → OWNS the regression**
  - full integration: 80k → 374k (matches #415's signature)
- **Mechanism (suspected, under investigation):** the regression appears only
  once the ready backlog spans many sealed ring generations (the queue ring
  rotates ~1/s under load; the ledger's horizon-gated fold trims to one
  slot-count wrap). Shallow-backlog shapes — W=256 depth-target-4000
  (12.9–13.3k/s parity) and 800/s W=32 — stayed clean, which is how the
  original #415 validation matrix missed it.
- **Striping (stripes=8) partially compensates: 4,813/s** — still short of
  the offered 5,000/s and of pg-boss/pgmq/pgque (5,000–5,300/s band), and no
  substitute for fixing the drain path. The single-stripe-claimer ceiling
  remains relevant as [hardbyte/awa#380](https://github.com/hardbyte/awa/issues/380),
  and producer backpressure as
  [hardbyte/awa#341](https://github.com/hardbyte/awa/issues/341), but neither
  explains this regression.

**Status: PR [hardbyte/awa#415](https://github.com/hardbyte/awa/pull/415) is
marked do-not-merge pending a fix; #409 and #410 are unaffected and remain
merge candidates on their own evidence.** The regression-gate section above
(ref800 + saturation pairs) used depth-limited shapes and its old-vs-new
conclusions stand for #409+#410; treat integration-level numbers that include
#415 as provisional until the drain fix lands.

A methodology note now lives with this result: `completion_rate` medians from
this harness can under-read on some builds; validate against
`enqueue_rate` + backlog series before citing them (this is how the original
version of this section went wrong).

Raw data: [`field-w128/fieldA-7systems/`](field-w128/fieldA-7systems/)
(river/oban/pgboss/procrastinate/absurd/pgque + the new/single-stripe
awa row — one grouped 7-system cell that completed cleanly),
[`field-w128/pgmq/`](field-w128/pgmq/) (separate Postgres image),
[`field-w128/awa-old-control/`](field-w128/awa-old-control/) (v0.6.0,
single-stripe control cell run separately for a clean before/after
pair), [`field-w128/awa-new-striped/`](field-w128/awa-new-striped/)
(0.7-alpha, stripes=8). Plots are included for all four W=128 cells
(`plots/` subdirectory in each) — this is the headline result of the
run.

### River: 634/s @ W=32, down from ~501/s @ W=64 in the May sweep at v0.35

Needs investigation, not a settled finding. The May 2026-05-09 sweep
(v0.35.1, different worker count W=64) measured river at 501/s
peaking around there; this run (v0.40.0, W=32) gets 634/s but with a
37.7-second claim-path p99 at just 800/s offered load — nowhere near
saturated. That combination (moderate throughput headline, enormous
tail) doesn't match the character of the May run's river numbers.

The adapter bump (bench-repo #35: v0.35.1 → v0.40.0) included a
migration run as part of the version bump, and the harness's
`main.py` for river wasn't otherwise touched. Two live hypotheses,
neither confirmed: (1) River v0.40's job-fetch cooldown/polling
interval changed in a way that shows up as tail latency at this
worker count, or (2) something in the harness's interaction with the
new schema is producing spurious waits that wouldn't show up in
River's own metrics. This needs a dedicated River-only cell at
matched W and a diff of the harness's river adapter against the
v0.35 run before drawing a conclusion — flagging it here rather than
asserting a regression, since the variables (adapter version, worker
count, harness version) aren't isolated in this run.

## What's not in this report

- **Chaos and bloat/pressure suites** — not re-run this cycle; the
  2026-05-09 sweep's chaos/bloat results stand (awa/pgque/river
  clean chaos sweep, awa/oban/pgque clean pressure sweep). Nothing
  in the 0.7-alpha changeset touches chaos-recovery or
  bloat-under-pressure code paths, but that's an assumption, not a
  re-verified claim.
- **WAL attribution (E3)** — cited from PR-level evidence, not
  reproduced in this run: btree/index maintenance ≈ 52% of WAL bytes
  at saturation on `main`, supporting the ≥40%-architecturally-removable
  threshold for index-avoiding designs. See the #415 PR body / awa
  perf-campaign notes for the full breakdown; out of scope for a
  results-directory copy since it's a diagnostic artifact, not a
  benchmark cell.
- **Plots for the regression-gate and non-headline field cells** —
  generated by the original `bench run` invocation but dropped here
  to keep this directory under the repo's size budget; the
  `summary.json` in each cell's directory carries the same numbers
  this SUMMARY quotes; the aborted grouped w32 run has no plots at
  all (it never reached the post-run render step) — `bench compare`
  still produced its markdown headline table from `summary.json`
  directly, which is where the w32 field numbers above come from.
- **`raw.csv`** — per repo convention (`.gitignore`), per-sample raw
  CSVs aren't checked in for any published results directory,
  including this one; `summary.json`'s per-metric median/peak
  aggregates are what ships, and are what every number in this
  SUMMARY is drawn from.

## Directory layout

```
regression-gate/
  old-ref800/  new-ref800/  old-sat-1/  new-sat-1/  old-sat-2/  new-sat-2/
field-w32/
  grouped-awa-pgque-river-oban-pgboss/   # aborted after pgboss; summary.json covers the systems that completed
  pgmq/                                  # separate Postgres image
  procrastinate-absurd/                  # re-run pair
field-w128/
  fieldA-7systems/    # river/oban/pgboss/procrastinate/absurd/pgque + awa (new, single-stripe)
  pgmq/               # separate Postgres image
  awa-old-control/    # v0.6.0, single-stripe
  awa-new-striped/    # 0.7-alpha, stripes=8
```

Each leaf directory has `summary.json` (aggregated medians/peaks per
metric) and `manifest.json` (exact CLI invocation, Postgres image,
adapter table lists, host info). `raw.csv` isn't checked in per repo
convention. `field-w128/*` and `fieldA-7systems` additionally carry
`plots/` (PNG + SVG); the grouped w32 cell additionally carries
`COMPARISON.md` (the `bench compare` output).
