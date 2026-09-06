# Reproducing AWA candidate measurements

Native AWA builds use `cargo build --release --locked`. The harness records the
resolved AWA source from `awa-bench/Cargo.lock`, hashes the executable and adapter
inputs, and verifies that receipt before launch. A neighboring `../awa` checkout
is not evidence of what ran. `--skip-build` refuses stale or unattributed native
artifacts. Docker metadata records the image identity without claiming a source
revision that has not been verified.

For paired runs, archive the executable together with its `.build.json` receipt,
then select it with `AWA_BENCH_EXECUTABLE=/absolute/path/awa-bench --skip-build`.
Changing the current dependency pin will not relabel that archived executable.
Build both baseline and candidate using the same adapter source and dependency
versions, changing only the AWA Git revision.

`scripts/run_awa_release_gate.py` runs alternating baseline/candidate cells at
800 jobs/s and saturation W=64/128/256, then a fresh candidate soak: 10 minutes
warmup, 10 minutes clean traffic, 60 minutes with an old transaction pinning the
MVCC horizon, and 30 minutes recovery. Each throughput cell gets a fresh PostgreSQL
instance. A separate protocol probe measures publication/reconciliation cost at
1/10/100 instances and 10/1,000/10,000 schedules; these are control-plane latency
measurements, not a job-throughput comparison. The probe requires an AWA revision
with owner reconciliation and is built with `--features cron-protocol --bin
awa-cron-protocol-bench`.

```bash
uv run python scripts/run_awa_release_gate.py \
  --baseline /path/to/baseline/awa-bench \
  --candidate /path/to/candidate/awa-bench \
  --protocol-bin /path/to/candidate/awa-cron-protocol-bench \
  --output results/YYYY-MM-DD-awa-release-gate
```

Add `--overnight` for matched four-hour baseline and candidate soaks after the
reference/saturation matrix: 10m warmup, 30m clean, 120m pinned transaction, and
80m recovery per build. This is eight hours of soak measurement plus the matrix.
The driver validates sampled horizon stability, pin age and post-release
advancement before accepting a soak. Failed pin validation preserves its evidence
and marks the campaign failed.

Each campaign keeps its build receipts, configuration, image identity, progress,
and per-cell manifests/summaries. Single paired cells are directional evidence;
report variation and repeat any suspicious difference before claiming a regression.
Fixed-rate cells fail validation when median enqueue rate is below 95% of the
requested load. A producer bottleneck must not silently turn an 800/s gate into
a lower-load run.

Generate the consistent comparison figures and measured report with:

```bash
uv run python scripts/plot_awa_campaign.py results/YYYY-MM-DD-awa-release-gate
uv run python scripts/report_awa_release_gate.py results/YYYY-MM-DD-awa-release-gate
```

The renderer produces PNG and SVG for throughput/latency, paired soak traces,
recovery detail and cron protocol cost. Baseline is blue/dashed and candidate is
orange/solid throughout the comparison figures. Soak traces use shared axes and
the original five-second samples, with no smoothing or interpolation. Phase
boundaries must agree within one sample. Recovery plots include each build's
actual clean threshold; crossing a threshold once does not establish sustained
recovery. The protocol heatmaps use separate labelled scales for different metrics.

The figure retains compact sampled series alongside the PNG, so it can be
regenerated without distributing the full raw CSV. Latencies are rolling-window
p99 samples; handler completion precedes the database completion batch commit.

SQLx 0.9.0 currently has a [reproduced TCP_NODELAY regression](../results/2026-09-06-sqlx-copy/SUMMARY.md).
The optional `awa-bench/sqlx-nodelay.toml` pins the merged upstream fix for
diagnostic comparisons. Resolve the lockfile with that Cargo config, then set
`AWA_BENCH_CARGO_CONFIG` to its absolute path when building through the harness.
Receipts capture the config and SQLx sources. Apply it equally to both builds,
and distinguish those results from the unpatched published dependency.

For two additional W128 pairs (candidate/baseline, then baseline/candidate), use
`scripts/repeat_awa_w128.py` with the same `--baseline`, `--candidate`, and a new
`--output` directory. It preserves the original executable receipts and uses a
fresh database for each 60s warmup + 180s measurement. Summarize all three pairs
with `scripts/report_awa_w128.py INITIAL_CAMPAIGN REPEAT_CAMPAIGN`.

Historical pre-review evidence includes the [earlier campaign and fresh soak](../results/2026-09-06-awa-481-nodelay/SUMMARY.md),
[W128 repeats](../results/2026-09-06-awa-481-w128-repeat/SUMMARY.md), and the
[isolated manifest-cache comparison](../results/2026-09-06-cron-manifest-cache/SUMMARY.md).
The soak report records a legacy horizon-age metric defect and independently
verified pin evidence; subsequent runs use the corrected query.

The current reviewed-head evidence is the [September 6–7 overnight report](../results/2026-09-06-awa-482-overnight/SUMMARY.md) and its [interpretation](../results/2026-09-06-awa-482-overnight/INTERPRETATION.md).
