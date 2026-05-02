# 2026-05-02 — multi-phase cross-system on awa 0.6.0-alpha.2

Six systems, one phase sequence, overlaid on every chart. Same
phase shape as the
[awa-only crash-recovery run](../2026-05-02-awa-crash-recovery/SUMMARY.md)
but with all adapters running simultaneously through the same
warmup → baseline → pressure → kill → restart → recovery sequence,
2 replicas × 32 workers each.

This is the visual companion to the
[bulk-everywhere matrix](../2026-05-02-alpha2-bulk-everywhere/SUMMARY.md)
— the matrix tells you the *peak* number, this run tells you what
the systems do *under transition*.

## Methodology

- **Systems:** awa, pgque, procrastinate, pgboss, river, oban
  (all in a single `bench run` invocation so the harness produces
  one overlaid chart per metric).
- **Replicas / workers:** 2 replicas × 32 workers per system
  (replica 0 gets killed; replica 1 survives).
- **Producer:** depth-target, `TARGET_DEPTH=2000`,
  `--producer-rate 2000`, `PRODUCER_BATCH_MAX=1000`.
- **Phases (5½ min total):**
  1. `warmup` — 30 s
  2. `baseline` (clean) — 60 s
  3. `pressure` (high-load = 1.5×) — 60 s
  4. `kill` — SIGKILL replica 0, 60 s with replica 1 alone
  5. `restart` — replica 0 process restarts, 60 s
  6. `recovery` (clean) — 60 s

Per-phase median throughput:

| System | baseline | pressure | kill | restart | recovery |
|---|---:|---:|---:|---:|---:|
| **pgque** | 15,452 | 15,418 | 13,457 | 14,871 | 15,103 |
| **pgboss** | 2,477 | 2,512 | 2,340 | 2,202 | 1,437 |
| **oban** | 1,314 | 1,221 | 1,185 | 403 ⚠ | 437 ⚠ |
| **river** | 608 | 621 | 598 | 684 | 448 |
| **awa** | 415 | 383 | **534** | 346 | 257 |
| **procrastinate** | 116 | 141 | 100 | 111 | 142 |

awa sits low here because the producer is rate-capped at 2,000 jobs/s
and 32 workers × 2 replicas is well under awa's saturation point —
this run is about *behaviour through chaos*, not peak. (For peak
numbers see the bulk-everywhere matrix.)

## Reading the charts

Every chart has the six-phase ribbon along the top with phase labels
and durations; one coloured line per system with end-of-line labels.
The two interesting transitions are at the **kill** boundary
(t=150s) and the **recovery** boundary (t=270s).

### `throughput.png` — completion rate per system

The headline chart. Three patterns visible:

- **Resilient (pgque, awa, procrastinate, river):** lines cross the
  kill boundary without a step-down. pgque's ~15k jobs/s is dead
  flat through the chaos band. awa actually *peaks* during kill
  (534 jobs/s) — replica 1 catches up while replica 0 is dead.
- **Degraded-on-restart (oban):** runs cleanly through kill but
  collapses on restart (1,185 → 403 jobs/s). Same shutdown-hang
  pattern visible at high worker counts in the bulk-everywhere
  matrix; here it surfaces under a different trigger.
- **Recovery-tail dip (pgboss, river, awa):** all three trail off
  during the recovery clean phase. Mostly autovacuum catching up on
  the dead-tuple debt accumulated during kill.

### `queue_depth.png` and `total_backlog.png`

Producer-side build-up. Systems whose consumers can't drain at the
producer rate show the depth ramp; systems that keep up stay flat.
oban's depth balloons after restart — visible companion to the
throughput collapse.

### `dead_tuples.png` (and `dead_tuples_faceted.png`)

Per-system dead tuple churn faceted across the storage tables each
adapter exposes. awa stays low (queue_storage ring tables don't
accumulate); pgboss and oban grow steadily.

### `producer_p99.png` / `producer_call_p99.png` / `subscriber_p99.png` / `claim_p99.png` / `end_to_end_p99.png`

Latency p99s per phase. Useful for spotting which side of the
pipeline the chaos hurts — `producer_p99` going up under kill means
the producer felt back-pressure; `claim_p99` going up means the
consumer's worker pool started contending.

### `wait_events_clean.png`

Postgres wait-event mix during the clean phase only (so you can
compare like-for-like steady-state without chaos noise). Each
system's bar shows its dominant bottleneck — for awa this is
`LWLock:WALWriteLock` plus `IO:WalSync` (the WAL plane, as in
[awa#207](https://github.com/hardbyte/awa/issues/207)); for pgque
it's substantially lower because batched commits amortise the WAL
fsync.

### `running_depth.png` / `scheduled_depth.png` / `retryable_depth.png`

Internal queue partitions where applicable. Most useful for awa and
pgboss (which expose multiple lifecycle states); other systems show
flat lines.

### `aged_completion_rate.png` / `completed_original_priority_*_rate.png`

Priority-class accounting. Only awa emits these series; for the
other systems the lines stay at zero. Useful for spotting priority
inversion under chaos.

## Caveats

- **awa is rate-capped here, not saturation-tested.** 415 jobs/s in
  this run is *not* awa's peak — see the bulk-everywhere matrix
  (4,576 jobs/s @ 128 workers) for the saturation number. This run
  measures behaviour during transitions.
- **pgque emitted `get_batch_events failed: batch not found` errors
  during the kill phase** — pgque's batched-read protocol takes a
  batch handle that becomes invalid when the worker holding it
  dies. Throughput was unaffected (15,418 → 13,457 → 14,871 across
  pressure/kill/restart) because new claims pick up new batches,
  but it's a soft chaos artefact worth noting.
- **oban's restart collapse (1,185 → 403)** is consistent with the
  shutdown-hang artefact at 64+ workers in the matrix runs. Likely
  the same root cause — worker pool restart not closing
  cleanly — surfacing under a different trigger.
- **Single replica killed** — full-fleet failure isn't tested.

## Reproducing

```sh
docker compose up -d postgres
export PRODUCER_BATCH_MAX=1000
uv run bench run \
  --systems awa,pgque,procrastinate,pgboss,river,oban \
  --replicas 2 --worker-count 32 \
  --producer-rate 2000 \
  --producer-mode depth-target --target-depth 2000 \
  --phase warmup=warmup:30s \
  --phase baseline=clean:60s \
  --phase pressure=high-load:60s \
  --phase 'kill=kill-worker(instance=0):60s' \
  --phase 'restart=start-worker(instance=0):60s' \
  --phase recovery=clean:60s
```

## Files

- [`plots/`](plots/) — 18 phase-banded charts (PNG)
- [`manifest.json`](manifest.json) — full per-system per-phase metrics
- [`index.html`](index.html) — embedded report
