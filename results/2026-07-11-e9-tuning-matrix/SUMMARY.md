# 2026-07-11 — E9 planner & storage micro-tuning matrix

Systematic A/B matrix applying less-common PostgreSQL planner/storage
knowledge to awa, per issue
[hardbyte/awa#419](https://github.com/hardbyte/awa/issues/419). Uses the
hardened bench protocol from the
[2026-07-11 0.7-alpha campaign](../2026-07-11-awa-07-alpha-gate/SUMMARY.md):
fresh database per cell, orphan sweeps before every cell, ref pre-builds,
and verdicts from the enqueue-rate series + backlog boundedness +
`pg_stat_wal` deltas rather than the completion-rate metric alone.

**Motivating data** (from the campaign's E3 WAL attribution): at load awa
is WAL-flush bound at the margin (`IO:WalSync` + `LWLock:WALWrite` ~ 40-50%
of active wait samples at overload) and B-tree index maintenance is ~52% of
WAL bytes. The hot path is PL/pgSQL, so plan-cache semantics apply.

## The short version

Five experiments, four config-only + one spike branch, all A/B'd against the
0.7-alpha baseline (`e389168`) at the 800/s W=32 gate cell and the 5000/s
W=128 cell, interleaved off/on/off. Headline: **one small adopt
(`wal_compression=lz4`), everything else a documented null — and the nulls are
the interesting finding, because they show the substrate's index design already
neutralizes the planner problems these knobs target.**

- **E9.1 group commit — REJECT (null).** `commit_delay` 500/2000µs is
  latency-neutral at the gate and shows *no* WalSync amortization at the 5k
  commit storm. awa's completion batcher already coalesces commits, so the knob
  has nothing left to batch. The 5k cell's wild backlog swings (125 → 32,700
  between "identical" cells) are drain-knee bistability, not a commit_delay
  effect — the higher-delay arm was one of the *healthy* draws.
- **E9.4a `wal_compression=lz4` — ADOPT (small, free).** Latency-neutral 2–6%
  WAL byte reduction. But it is *not* the WAL diet: it only shrinks full-page
  images, and the ~52% index-maintenance WAL is ordinary `INSERT_LEAF`
  records, not FPIs. Recommend as an ops default with that caveat stated.
- **E9.3 plan-cache audit — no pinning, no code change (positive null).** On a
  305k-row backlog, `claim_ready_runtime`'s inner statements underestimate rows
  by 512× — but the claim CTE is index-forced by its `ORDER BY lane_seq LIMIT`,
  so the estimate never changes the plan, and there is no generic-plan
  misplanning across the 5-execution boundary. The substrate design already
  immunizes the claim path.
- **E9.2 statistics bundle (spike) — REJECT (null), as E9.3 predicted.**
  Extended stats + insert-driven autovacuum verified created and populated, but
  neutral on throughput/latency: the claim path they'd help is
  estimate-insensitive.
- **E9.4b BRIN — design sketch only (rejected on the claim path).** BRIN can't
  serve the equality-prefix + ordered-LIMIT claim; the only worthwhile BRIN is
  a follow-up spike on `done_entries` cold indexes.

**Net for v0.7 tuning**: ship `wal_compression=lz4` as a deployment default;
do not ship group commit, plan_cache_mode pinning, or the statistics bundle.
The structural WAL lever (#295) and a `done_entries` BRIN spike are the real
next moves on WAL; none of the planner knobs are.

## Environment

24-core / 98GB host, Docker Compose. PostgreSQL capped to 4 CPU / 8GB per
the canonical harness config, `postgres:18.3-alpine`, port 15555, db
`awa_bench`, user `bench`. Fresh database per cell — no cross-cell state.
`JOB_WORK_MS=1` (handler does negligible work; the queue engine's own
overhead is what's measured).

**Baseline ref for every A/B: `perf/07-alpha-integration@e389168`** — the
0.7-alpha stack. Its orphan-free reference numbers (from the campaign):
800/s W=32 gate -> 802/s, p99 21ms; 5k/s W=128 -> 5,000/s bounded (backlog
~125), p50 13ms, p99 30.5ms; WAL 1,142 MB per 5k cell; saturation W=256
depth-target -> 10.6k/s p99 333ms. The bench rebuilds `awa-bench` from the
sibling checkout `/home/brian/dev/awa` via path deps; this ref was
pre-built once before the measured sequence and every cell ran
`--skip-build` (the harness still does one idempotent cache-hit rebuild at
run start).

**Cell length caveat**: dev-machine cells — 30s warmup + 180s clean phase.
Directional, not the 20-minute published-sweep length. Verdicts rest on
paired same-length interleaved cells (off/on/off), which averages out
host-level drift.

**GUC injection**: config-only experiments extend the uncommitted
`docker-compose.override.yml` (auto-merged by the harness's bare
`docker compose`); `postgres.conf` itself stays pinned for run-over-run
comparability. Each cell's exact override is archived at
`cells/<cell>/docker-compose.override.yml` and the effective GUCs at
`cells/<cell>/gucs.txt`. Per-experiment override files live in `overrides/`.

## E9.1 — Group commit

`commit_delay` (microseconds) + `commit_siblings=5`. A commit leader with
>=5 concurrent active transactions sleeps `commit_delay` before flushing,
coalescing the 128-backend commit storm into fewer WAL fsyncs. It adds
latency by design — the gate cell's p50/p99 is the guard; the 5k cell's
`IO:WalSync` wait-sample count is the amortization signal.

**GUCs verified per cell** (`cells/*/gucs.txt`): off cells `commit_delay=0`,
on cells `commit_delay=500` / `2000`, `commit_siblings=5` throughout.

### Gate cell (800/s, W=32) — interleaved off/500/off/2000/off

| Cell | enqueue/s | compl/s | backlog | p50 | p99 | WAL MB | WalSync | WALWrite | samples |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| off #1 | 798 | 800 | 0 | 12.0 | 20.0 | 157 | 66 | 0 | 139 |
| **500µs** | 798 | 798 | 0 | 12.0 | 21.0 | 158 | 68 | 15 | 219 |
| off #2 | 798 | 800 | 0 | 12.0 | 20.5 | 157 | 63 | 0 | 136 |
| **2000µs** | 798 | 798 | 0 | 12.5 | 21.0 | 158 | 87 | 23 | 374 |
| off #3 | 799 | 799 | 20 | 16.5 | 38.0 | 157 | 116 | 38 | 279 |

Gate verdict: **latency-neutral**. commit_delay up to 2000µs holds p50 at
12–12.5ms and p99 at 21ms against the two clean off controls' 20–20.5ms. The
off #3 excursion (p99 38ms, backlog 20, WalSync 116) is host drift landing on
the last-in-sequence cell, not a group-commit effect — the on cells that
bracket it are clean, which is exactly why the protocol takes multiple
interleaved controls. At W=32 the commit_siblings=5 gate is met but the
coalescing window captures too few concurrent commits to move WAL fsync
counts (WalSync 66→68→63 across off/500/off). The knob is inert here, as
designed; the 5k commit storm is where it can pay off.

### 5k cell (5000/s, W=128) — interleaved off/500/off/2000/off

| Cell | enqueue/s | compl/s | backlog | p50 | p99 | WAL MB | WalSync | WALWrite | samples |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| off #1 | 4990 | 5005 | 718 | 104.1 | 235.0 | 793 | 141 | 82 | 386 |
| **500µs** | 4992 | 5004 | **32700** | 6572 | 6722 | 796 | 134 | 76 | 387 |
| off #2 | 4989 | 4989 | 125 | 16.0 | 28.0 | 746 | 75 | 13 | 200 |
| **2000µs** | 4989 | 5001 | 125 | 15.0 | 27.5 | 740 | 74 | 12 | 239 |
| off #3 | 5000 | 5000 | 125 | 14.5 | 27.0 | 743 | 47 | 8 | 209 |

5k verdict: **null result — no detectable WalSync amortization; reject.** The
headline is the variance *within* the off controls: backlog 718 / 125 / 125,
p99 235 / 28 / 27ms, WalSync 141 / 75 / 47. The 5k/W=128 fixed-rate cell sits
right at awa's single-stripe drain knee (the #380 / #418 regime), so a cell is
bistable — a "stressed" run (off #1, ~386 active wait samples) buffers deep,
a "healthy" run (off #2/#3, ~200 samples) holds backlog at ~125. The two
group-commit arms land inside that off spread: 2000µs was one of the healthy
runs (backlog 125, WalSync 74 — indistinguishable from off #2), and the
500µs blowup to backlog 32,700 is a stressed-run draw, **not** a commit_delay
effect — if the delay caused it, the 2000µs arm (larger delay) would be worse,
and it was the opposite. WalSync counts track run health, not commit_delay
(healthy off #3 = 47; stressed off #1 = 141). commit_delay is inert here
because awa's completion batcher already coalesces the commit storm — the
per-commit fsync the knob targets has mostly already been amortized upstream.
Enqueue rate is a rock-solid 4990–5000/s in every cell (producer never the
bottleneck), so the whole signal lives in drain/backlog, which the knob
doesn't touch.

**E9.1 overall: REJECT.** Latency-neutral at the gate (safe to leave off),
zero measurable benefit at the commit storm. Not worth the added tail-latency
risk of a non-zero `commit_delay` in production. Recommend documenting *why*
(the batcher already coalesces) rather than shipping the knob.

## E9.4a — WAL compression (lz4)

`wal_compression=lz4` compresses full-page images written to WAL. Direct
attack on the FPI share of WAL bytes. Verdict = `pg_wal_bytes_delta` /
FPI-delta reduction with throughput/p99 neutrality.

**GUCs verified**: on cells `wal_compression=lz4`, off cells `off`.

### Gate cell (800/s, W=32) — interleaved off/on/off

| Cell | enqueue/s | compl/s | p50 | p99 | WAL MB | WAL recs | WAL FPI |
|---|---:|---:|---:|---:|---:|---:|---:|
| off #1 | 800 | 800 | 12.0 | 20.0 | 158 | 768,330 | 9,416 |
| **lz4** | 798 | 798 | 12.0 | 21.0 | **148** | 771,414 | 9,380 |
| off #2 | 798 | 799 | 12.0 | 20.0 | 158 | 769,492 | 9,267 |

Gate verdict: **~6% WAL byte reduction, fully latency-neutral.** 148 MB on vs
158 MB for both off controls (which agree, so the delta is real, not drift),
at identical record count (~769k) and identical p50/p99 (12ms / 20–21ms). The
FPI *count* is unchanged (~9,400) — lz4 shrinks each FPI's bytes, it doesn't
remove FPIs. 6% is modest here because the gate cell's WAL is
record-dominated, not FPI-dominated; the FPI share is larger under the 5k
load below.

### 5k cell (5000/s, W=128) — interleaved off/on/off

| Cell | enqueue/s | compl/s | backlog | p50 | p99 | WAL MB | WAL recs | WAL FPI |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| off #1 | 4989 | 4991 | 0 | 15.0 | 27.0 | 738 | 3,089,487 | 10,716 |
| **lz4** | 4989 | 4989 | 0 | 16.0 | 28.0 | **722** | 3,100,745 | 11,216 |
| off #2 | 4990 | 4990 | 61 | 15.0 | 27.0 | 743 | 3,098,153 | 11,079 |

5k verdict: **~2.5% WAL byte reduction, latency-neutral.** 722 MB on vs 738 /
743 MB for the two off controls (which agree), same record count (~3.1M) and
same p50/p99 (15–16ms / 27–28ms). FPI *count* is flat across all three
(10.7k–11.2k) — lz4 shrinks FPI bytes, not FPI count, as expected.

**E9.4a overall: ADOPT (small, free win) — but it is not the WAL diet.** lz4
is a real, latency-neutral 2–6% WAL byte reduction with negligible CPU, safe
to recommend as a deployment default. But it is *not* the ~52% lever: the E3
attribution showed B-tree `INSERT_LEAF` records are ~52% of WAL *bytes*, and
those are ordinary WAL records, not full-page images — lz4 only touches FPIs,
which are a minority of steady-state WAL bytes here (the FPI-to-total ratio is
why the gate cell's 6% is *higher* than the 5k cell's 2.5%: fewer records per
checkpoint window at the gate makes FPIs a larger share). The ~52% index-WAL
lever needs an index-avoiding storage design (#295) or BRIN (E9.4b), not
compression. Recommend `wal_compression=lz4` as an ops-handbook default with
that caveat stated explicitly so it is not mistaken for the structural fix.

## E9.3 — Plan-cache audit

Method: a `KEEP_DB` deep-backlog cell (8000/s, W=128, 90s clean — enqueue
exceeds the ~5k single-stripe drain so backlog accrues) built **305,019 live
`ready_entries` rows**, then `claim_ready_runtime('awa_longhorizon_bench',
512, 0, 0)` was driven 8× in one psql session with
`auto_explain.log_nested_statements=on`, `log_analyze=on`, `log_min_duration=0`
— so every inner statement's plan + est/actual rows is logged, and calls 6–8
cross PostgreSQL's 5-execution custom→generic plan boundary. Artifacts:
`artifacts/e93_manual_drive.txt`, `artifacts/e93_autoexplain_pglog_full.txt`,
`artifacts/e93_drive.sql`. **Diagnostic only** — `log_analyze` adds
EXPLAIN ANALYZE overhead, so timings here are not a throughput verdict.

Per-call timing (claimed count alternates 512/0 with the sealed-generation
rhythm): 68 / 9 / 56 / 6 / 55 / 18 / 55 / 55 ms. **No cliff at call 6** — the
generic-plan executions (6–8) time identically to the custom ones (1–5).

The claim CTE inner plan on the 305k-row backlog (`ready_slot` partition 3):

| Node | est rows | actual rows |
|---|---:|---:|
| `Index Scan idx_awa_ready_3_lane_shard` on `ready_entries_3` | **1** | **512** |
| `Limit` → `WindowAgg` → `Subquery Scan selected` | 1 | 512 |
| `Index Scan idx_awa_ready_tombstones_3_lane_shard` (correlated) | 1 | 0 (×512 loops) |
| `Index Scan idx_awa_ready_claim_attempt_batches_3_lane` (correlated) | 1 | 1 (×512 loops) |

Findings:

1. **No sequential scans on any hot table.** Every access to `ready_entries`,
   `ready_tombstones`, and `ready_claim_attempt_batches` is an index scan on
   the shard-aware composite indexes. The only `Seq Scan` in the whole trace is
   on a one-row sequence-state table.
2. **Row estimates *are* badly wrong** — the `ready_entries` index scan
   estimates 1 row where 512 are read (a 512× underestimate), and it propagates
   up the Limit/WindowAgg/Aggregate chain as `rows=1`. This is exactly the
   estimate whipsaw E9.2 anticipated.
3. **…but the misestimate is operationally inert.** The composite index
   `(queue, priority, enqueue_shard, lane_seq)` exactly covers the CTE's
   equality predicates *and* its `ORDER BY lane_seq ASC LIMIT`, so the planner
   takes an index-ordered scan with early-LIMIT termination **regardless** of
   the estimated row count — a correct 512-row estimate would pick the byte-for-
   byte identical plan. The nested EXISTS probes are correlated index scans
   (the right shape). There is no join-order or scan-method decision here for a
   better estimate to flip.
4. **No generic-plan misplanning caught.** Custom and generic executions
   produced the same plan and the same timing across the 5-execution boundary.

**E9.3 verdict: no `plan_cache_mode` pinning warranted, and no code change.**
The hot claim path is estimate-insensitive by construction (index-forced by the
ORDER BY + LIMIT). This is a *positive* structural result — the substrate's
index design already immunizes the claim path against the generic-plan flip and
against stale statistics. It also predicts E9.2 will be a null on the claim
path (below).

## E9.2 — Statistics bundle (spike branch)

Spike branch `spike/e9-statistics` (commit
[`290ae31`](https://github.com/hardbyte/awa) — SPIKE, not for merge, no
tests/TLA) amends `install_queue_storage_substrate` with, per installed schema
(round-1 scope — no `n_distinct` overrides, no parent-ANALYZE):
- `CREATE STATISTICS (ndistinct, dependencies, mcv)` on
  `(queue, priority, enqueue_shard)` per `ready_entries` partition, and
  `(queue, priority)` on `queue_lanes`;
- `CREATE STATISTICS (ndistinct, dependencies)` on
  `(ready_slot, ready_generation)` per `ready_entries` partition;
- `autovacuum_vacuum_insert_threshold=10000` +
  `autovacuum_vacuum_insert_scale_factor=0.05` on `ready_entries` /
  `done_entries` partitions.

The "off" arm is the e389168 baseline (the healthy `gc_*_off` / `lz4_*_off`
cells); the "on" arm is the spike build. Two on-reps per cell.

| Cell | enqueue/s | compl/s | backlog | p50 | p99 | WAL MB |
|---|---:|---:|---:|---:|---:|---:|
| gate off (baseline ×2) | 798 | 800 | 0 | 12.0 | 20.0–20.5 | 157 |
| **gate stats on1** | 800 | 799 | 0 | 12.0 | 25.0 | 159 |
| **gate stats on2** | 798 | 798 | 0 | 13.0 | 23.0 | 158 |
| 5k off (baseline ×3) | 4989–5000 | — | 0–125 | 14.5–16 | 27–28 | 738–746 |
| **5k stats on1** | 4989 | 4991 | 0 | 15.0 | 28.0 | 739 |
| **5k stats on2** | 4990 | 5012 | 151 | 57.5 | 158.1 | 776 |

**Spike verified applied** (`artifacts/e92_stats_verify.txt`): all 33 extended
statistics objects exist and are **populated** (ndistinct + dependencies + mcv
present on the 16 `st_ready_N_qpe`; ndistinct + dependencies on the 16
`st_ready_N_slotgen`), and the insert-driven autovacuum reloptions are set on
every `ready_entries` / `done_entries` partition. The one exception:
`st_queue_lanes_qp` exists but is **not populated** — `queue_lanes` is a
handful of rows, below the analyze threshold, so it never gets built (harmless,
and a hint that the queue_lanes stat is pointless at this table size).

E9.2 verdict: **null result — neutral, no adopt.** Gate cells are within the
gate's noise band (p99 23–25ms on vs 20–20.5ms off — compare the off #3
excursion to 38ms in E9.1, same envelope). 5k on1 is a clean healthy draw
(backlog 0, p99 28ms, indistinguishable from the off cells); 5k on2 is a
mildly-stressed knee draw (backlog 151, p99 158ms) that sits inside the same
drain-knee bistability the E9.1 off controls showed (off #1 = 718 backlog).
There is no systematic shift. This is the *expected* outcome given E9.3: the
claim hot path is index-forced by its `ORDER BY lane_seq LIMIT`, so better row
estimates cannot change its plan — and the claim path is where the load is.
The statistics are correctly built and populated; they simply have nothing to
fix on the paths that matter at this workload. **Reject for adoption**; keep
the spike branch as evidence. (A caveat: at 180s these partitions are freshly
analyzed anyway; a longer-horizon soak with churn — where the issue's "estimates
whipsaw" concern actually bites — could still find value, but that is a
separate, longer experiment, and E9.3 makes it a low-probability bet for the
claim path specifically.)

## E9.4b — BRIN spike (design sketch)

Not benchmarked (time budget spent on E9.1–E9.3 + E9.2). Design analysis
against the E3 rmgr attribution and the claim predicate:

The ~52% WAL-btree-maintenance cost is dominated by the per-partition
`_lane_shard` composite btrees `(queue, priority, enqueue_shard, lane_seq)` on
`ready_entries`, `done_entries`, `ready_tombstones`, and `leases`. The
tempting BRIN target is `lane_seq`: it is monotone within a
`(queue, priority, enqueue_shard)` lane and jobs land in a `ready_slot`
partition in roughly enqueue (≈ lane_seq) order, so heap-page order correlates
with `lane_seq` — the precondition BRIN needs.

**But BRIN cannot replace these btrees**, for two structural reasons:

1. **The claim predicate is an equality prefix + ordered range + LIMIT.**
   `WHERE queue=? AND priority=? AND enqueue_shard=? AND ready_slot=? AND
   ready_generation=? AND lane_seq>=? ORDER BY lane_seq ASC LIMIT n`. The btree
   serves this as a single index-ordered scan that stops after `n` rows
   (E9.3 confirmed: `Index Scan … LIMIT`, 512 rows read, early termination).
   BRIN offers neither the equality lookup nor ordered retrieval — it returns
   candidate block ranges, forcing a heap scan + sort to satisfy the ORDER BY,
   which destroys the LIMIT early-out. On a 300k-row backlog partition that is
   a full-partition scan per claim.
2. **Multiple lanes interleave in one partition.** `queue`, `priority`,
   `enqueue_shard` are low-cardinality and *not* correlated with heap order, so
   BRIN can prune on `lane_seq` range only — it cannot isolate one lane's rows.
   With many queues/priorities sharing a `ready_slot`, the overlapping-range
   block set is most of the partition.

**Where BRIN *could* pay** without touching the claim hot path: a
**secondary** BRIN on `done_entries(created_at)` or `done_entries(job_id)` for
the cold compatibility-deletion / audit scans, replacing the
`idx_%s_done_%s_job` btree there — `done_entries` is append-only and never
claim-scanned, so losing ordered/point lookup is cheap, and a BRIN is
~1000× smaller with near-zero maintenance WAL. Estimated WAL saving: the
`done` job-index INSERT_LEAF records are a slice of the 52% btree share
(roughly a third of the ready/done/tombstone index writes); a BRIN there would
remove most of that slice's per-insert WAL. This is the only BRIN worth a real
spike, and it is a follow-up, not part of this matrix.

**Recommendation**: reject BRIN on the claim-path `_lane_shard` indexes
(structurally incompatible with the ordered-LIMIT claim). File a separate spike
for a BRIN on the `done_entries` cold indexes as a WAL-diet follow-up to #295.

## Recommendations

| Item | Verdict | Action |
|---|---|---|
| `wal_compression=lz4` | **Adopt** | Deployment default; ops-handbook entry (#379) + `awa doctor` advisory check (#373). Not a substitute for #295. |
| `commit_delay` / `commit_siblings` | **Reject** | Leave at defaults (0 / 5). Document *why* (batcher already coalesces) so it is not re-proposed. |
| `plan_cache_mode` pinning | **Reject** | No change — claim path is estimate-insensitive by index design (E9.3). |
| Extended statistics bundle (E9.2) | **Reject** | Do not merge the spike. Neutral at this workload; revisit only if a long-horizon churn soak shows estimate whipsaw on a *non-claim* path. |
| Insert-driven autovacuum on ready/done partitions | **Reject for now** | Bundled into the E9.2 null; no measured VM/index-only-scan win at 180s. Could re-test in isolation on a long soak. |
| BRIN on claim-path `_lane_shard` indexes | **Reject** | Structurally incompatible with the ordered-LIMIT claim. |
| BRIN on `done_entries` cold indexes | **Needs-more** | File a follow-up WAL-diet spike (companion to #295). |

## For the ops handbook (#379) / awa doctor (#373)

- **`wal_compression=lz4` as a recommended deployment default.** Latency-
  neutral, 2–6% WAL byte reduction on this workload, negligible CPU. Frame it
  honestly: it compresses full-page images only, so the win is modest and it is
  *not* a fix for index-maintenance WAL volume — that is the #295 territory. An
  `awa doctor` check could advise enabling it when `SHOW wal_compression` is
  `off` (informational, not a hard failure).
- **`commit_delay` is a trap for this engine.** If an operator reaches for
  group commit to cut WAL fsyncs, the handbook should note that awa's
  completion batcher already coalesces the commit storm, so `commit_delay` adds
  tail latency for no fsync reduction — measured here. Recommend leaving it 0.
- **Do not pin `plan_cache_mode`.** The claim path's plans are forced by the
  substrate's composite `(queue, priority, enqueue_shard, lane_seq)` indexes +
  `ORDER BY … LIMIT`; row-estimate errors (even 512×) don't change them. A
  well-meaning `plan_cache_mode=force_custom_plan` would only add per-call
  planning cost with no benefit.
- **`random_page_cost` / `effective_io_concurrency` (E9.5, not measured here):**
  the bench box reports `random_page_cost=4`, `effective_io_concurrency=16`.
  For NVMe production, `random_page_cost≈1.1` is the standard guidance; it was
  out of scope for this config-only matrix (the canonical `postgres.conf` stays
  conservative for run-over-run comparability) but belongs in the handbook as a
  deployment note. No awa-specific measurement was taken.

## Reproducing / artifacts

- `run_cell.sh` — per-cell runner (orphan sweep + `down -v` + `--skip-build` +
  GUC capture + summary/manifest archive).
- `overrides/` — exact compose override per experiment (the GUC diff).
- `drive_config_matrix.sh`, `drive_e92_spike.sh`, `e93_planaudit.sh` — the
  three drivers.
- `extract.py` — pulls the verdict row (enqueue/backlog/latency/WAL/wait
  counts) from a cell's `summary.json`.
- `cells/<cell>/` — `summary.json`, `manifest.json`, `docker-compose.override.yml`,
  `gucs.txt`, `run_dir.txt` per cell.
- `artifacts/` — E9.3 plan-audit (`e93_manual_drive.txt`,
  `e93_autoexplain_pglog_full.txt`, `e93_drive.sql`, `e93_backlog.txt`) and
  E9.2 spike verification (`e92_stats_verify.txt`).
