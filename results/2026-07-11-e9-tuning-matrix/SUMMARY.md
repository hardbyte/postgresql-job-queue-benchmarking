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

_(filled after all cells land — see per-experiment verdicts below)_

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

_(pending)_

## E9.4a — WAL compression (lz4)

`wal_compression=lz4` compresses full-page images written to WAL. Direct
attack on the FPI share of WAL bytes. Verdict = `pg_wal_bytes_delta` /
FPI-delta reduction with throughput/p99 neutrality.

_(table pending)_

## E9.3 — Plan-cache audit

_(pending)_

## E9.2 — Statistics bundle (spike branch)

_(pending)_

## E9.4b — BRIN spike

_(pending / design sketch)_

## Recommendations

_(pending)_

## For the ops handbook (#379) / awa doctor (#373)

_(pending)_
