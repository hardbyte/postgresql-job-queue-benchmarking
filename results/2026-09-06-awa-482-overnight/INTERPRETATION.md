# Reviewed AWA candidate: overnight interpretation

The campaign completed successfully in 8h 44m, from September 6 at 22:23 NZST
to September 7 at 07:07 NZST. All ten throughput/soak cells and nine cron-probe
configurations completed. The driver exited 0. Both four-hour soaks validated
1,440 pinned samples, with the same horizon held for approximately 7,199 seconds
and advancement observed after release. See [measured tables and figures](SUMMARY.md).

## What this supports

The reviewed candidate `59a1377` handles the 800/s reference and two-hour MVCC pin
without sustained queue accumulation in this experiment. During the pin, main
`49b1a77` and candidate record approximately 800 handler completions/s, median
queue depth zero and median sampled window-p99 latency of 44ms and 46ms.
The queue has brief nonzero samples; a zero median is not a claim that it was
always empty. Completion is observed before the database batch commits.

Both builds accumulate almost the same estimated dead tuples during the pin:
peaks of 7,469 and 7,413. The largest per-table peak is `awa.claim_ring_slots`
at 7,193 in both. This is shared storage metadata behaviour, not evidence of
unbounded job-row bloat introduced by owner reconciliation. Tracked table/index
relation sizes remain below 25 MiB throughout both traces; this does not include
all database disk or WAL and is not a bound for larger payloads or other rates.

The candidate's 100-runtime/10,000-schedule steady publication p99 is 300.8ms;
reconciliation takes 65.0ms. Publication at 100 runtimes is around 300–332ms
across all schedule counts. This is consistent with fleet serialization dominating
these steady-publication measurements. It is an inference from the probe, which
includes lock wait, not a causal profile or a v044-to-v045 lock-overhead comparison.

## Differences to retain

| Saturation | Main completions/s | Candidate completions/s | Candidate delta |
| --- | ---: | ---: | ---: |
| W64 | 4,521.7 | 4,804.6 | +6.3% |
| W128 | 7,632.8 | 7,347.4 | −3.7% |
| W256 | 10,859.4 | 10,505.9 | −3.3% |

Sampled p99 latency is lower on candidate in these three cells. Each workload
has one sequential pair; the measurements do not establish a significant gain
or regression. Previous W128 repeats measured `a8e7e63`, not this reviewed head,
so they cannot be pooled as repeats of `59a1377`.

Recovery is visibly different. Main is already below 10% of its pinned dead-tuple
peak at the first recovery sample; candidate first reaches that threshold after
45 seconds. The stricter clean thresholds are **112.2 tuples for main** and
**138.6 for candidate** (1.1 × each build's clean median). Main first crosses after
35 seconds; candidate after 1,185 seconds (19m 45s).

This is not a 20-minute queue stall. Throughput continues near 800/s and median
queue depth stays zero while candidate's estimated dead-tuple count spends the
first ~20 minutes mostly around 200–300. Both traces later rise above their clean
thresholds again. Their recovery medians are 130.5 and 144.5 tuples, both above
those thresholds. Report **first crossing**, not sustained return to baseline.
The [recovery figure](plots/recovery.png) shows both the first five minutes and
the complete 80-minute window. Five-second samples and PostgreSQL statistics
limit the precision of these timings; zero means the first sample, not instant
vacuum at transaction release.

Candidate's clean-soak median window-p99 is 25ms versus main's 43ms, while their
pinned and recovery medians are much closer. Since the soaks ran sequentially,
this clean-phase difference cannot be attributed to the code change alone.

## Release decision and next evidence

[Full 29-job CI](https://github.com/hardbyte/awa/actions/runs/34026929816) and the
[release gate](https://github.com/hardbyte/awa/actions/runs/34026762355) passed on
exact candidate `59a1377`, including released-artifact rehearsals and model checks.
The separate [earlier pinned-wheel teardown failure](https://github.com/hardbyte/awa/actions/runs/34025905890/job/101466701977)
still has no demonstrated native root-cause fix; a later pass does not erase it.

Keep the reviewed implementation moving through PR review. These data do not
establish a blocking throughput collapse or sustained job backlog under the tested
pin. Before claiming a small performance regression or improvement, repeat W128
and W256 on the same head with balanced order. Repeat the recovery experiment
with balanced soak order and table-level vacuum evidence if recovery to the clean
threshold is a release criterion; this pair alone does not identify its cause.

Both binaries use the same pinned SQLx TCP_NODELAY fix at upstream commit
`6e57d05490859f31aa364ca69fcb379f3a2995e6`. The default harness lock and AWA dependency
remain published SQLx 0.9. The [published-driver reproduction](../2026-09-06-sqlx-copy/SUMMARY.md)
is a separate release concern: this overnight campaign does not validate the
shipping unpatched dependency. Require a fixed published dependency and repeat
shipping-candidate validation before release. The August 23 soak is not reused.
