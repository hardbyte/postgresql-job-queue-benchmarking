# 2026-06-05 - Awa v0.6 gate: sequence cursors + striped counters

Long-horizon release-gate run for the v0.6 storage work. This compares the
current Awa #295/#290 branch against the latest vendored pgque submodule at
the same offered rate.

| field | value |
|---|---|
| run id | `custom-20260605T013419Z-0c45d0` |
| awa revision | `7ddacce` on `issue-295-v06-sequence-cursors` with local dirty changes |
| pgque submodule | `55ddc1d` (`v0.2.0-rc.2-58-g55ddc1d`) |
| shape | 1 replica, 32 workers, 800 jobs/s fixed-rate producer |
| phases | warmup 5m, clean 20m, idle-in-tx 60m, recovery 10m |
| raw artifacts | `results/custom-20260605T013419Z-0c45d0/` |

Command:

```bash
CARGO_TARGET_DIR=/home/brian/dev/postgresql-job-queue-benchmarking/target/codex-bench \
uv run bench run \
  --systems awa,pgque \
  --producer-rate 800 \
  --worker-count 32 \
  --replicas 1 \
  --sample-every 5 \
  --phase warmup=warmup:5m \
  --phase clean_1=clean:20m \
  --phase idle_1=idle-in-tx:60m \
  --phase recovery=clean:10m
```

## Headline

Both systems completed the 60-minute pinned-MVCC phase without a sustained
throughput cliff.

The important Awa result is that the old per-row metadata wall is gone in this
shape. The earlier diagnostic run with only sequence lane cursors moved the
cliff to `queue_terminal_live_counts`; striping the terminal live counter by
`job_id % 256` removed that next hot-row chain. Awa now holds the 800/s offered
rate through a full one-hour pinned transaction with bounded backlog.

Pgque also holds the same 800/s gate. Its tradeoff in this run is storage and
latency shape: the active event table grows to roughly 1.3-1.4 GB during and
after the pinned phase, and p95 latency rises during the pin, but throughput and
depth remain stable.

## Phase Medians

| system | phase | completion/s median | enqueue/s median | queue depth p95 | queue depth max | e2e p95 median | e2e p95 p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| awa | clean_1 | 798.49 | 798.48 | 20 | 438 | 19.01 ms | 158.08 ms |
| awa | idle_1 | 798.57 | 798.59 | 20 | 2300 | 30.02 ms | 216.06 ms |
| awa | recovery | 798.61 | 798.46 | 20 | 431 | 24.02 ms | 343.04 ms |
| pgque | clean_1 | 799.98 | 800.13 | 0 | 81 | 110.21 ms | 111.89 ms |
| pgque | idle_1 | 800.05 | 800.13 | 81 | 101 | 131.55 ms | 148.05 ms |
| pgque | recovery | 801.87 | 800.43 | 0 | 80 | 115.46 ms | 122.85 ms |

Notes:

- Awa had a few bounded latency/depth spikes, including a max sampled depth of
  2300 in `idle_1`; the distribution stayed flat enough that p95 depth was 20
  and the run ended the pinned phase at depth 20.
- Pgque depth was tighter, but latency was consistently higher in this
  fixed-rate shape.
- Recovery was clean for both systems after the idle transaction released.

## Storage Observations

The raw sampler preserved pgque's event-table growth:

| moment | pgque active event table |
|---|---:|
| ~30m pinned | ~878 MB, ~2.64M live rows |
| ~50m pinned | ~1.20 GB, ~3.61M live rows |
| late recovery | ~1.43 GB, ~4.29M live rows |

The run also exposed a sampler gap on the Awa side: the descriptor did not
include `queue_terminal_live_counts`, `queue_terminal_rollups`,
`queue_enqueue_heads`, or `queue_claim_heads`, so the raw CSV cannot preserve
the exact terminal-counter table stats that motivated the striping fix. The
adapter descriptor has been updated after this run so future long-horizon runs
sample those tables directly.

## Interpretation

For v0.6, the release gate should move from "does pinned MVCC trigger a
metadata cliff?" to "document the MVCC discipline and storage shape." The
sequence cursor + striped terminal counter design handles the one-hour
idle-in-transaction gate at 800/s on this workstation.

Remaining follow-ups:

- Re-run this exact gate once the Awa changes are committed cleanly, so the
  manifest records a non-dirty revision and captures the newly added Awa table
  sampler targets.
- Run a higher offered-rate sweep to find the new ceiling. This run proves the
  old cliff is gone at 800/s; it does not prove the absolute throughput limit.
- Keep the #295 rotation/TRUNCATE design notes, but scope them as the next
  storage-efficiency step rather than a blocker for this specific 800/s
  pinned-MVCC gate.
