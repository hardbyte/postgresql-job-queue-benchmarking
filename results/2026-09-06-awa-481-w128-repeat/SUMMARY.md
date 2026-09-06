# W128 saturation repeats

Same archived main (`49b1a77`) and #481 (`a8e7e63`) executables as the initial campaign, both using the pinned upstream SQLx socket fix. Fresh PostgreSQL 18.3 per cell; W128, depth target 4,000, offered ceiling 50,000/s, 60s warmup and 180s measurement. Pair order: baseline/candidate, candidate/baseline, baseline/candidate. No local compilation or other benchmark ran concurrently.

Rates are handler completions before database completion-batch commit. Latency is the median of rolling 30-second p99 samples at five-second cadence, not a job-level aggregate p99.

| Pair | Build | Complete/s | E2E p99 ms | Queue depth |
| --- | --- | ---: | ---: | ---: |
| 1 | baseline | 7,657.7 | 617.2 | 2,432.0 |
| 1 | candidate | 7,512.3 | 743.4 | 2,464.0 |
| 2 | baseline | 7,554.9 | 543.7 | 2,464.0 |
| 2 | candidate | 7,165.4 | 528.4 | 2,560.0 |
| 3 | baseline | 7,315.7 | 684.5 | 2,480.0 |
| 3 | candidate | 7,186.9 | 668.7 | 2,432.0 |

| Pair | Throughput delta | Latency delta |
| --- | ---: | ---: |
| 1 | -1.9% | +20.4% |
| 2 | -5.2% | -2.8% |
| 3 | -1.8% | -2.3% |

Across the three pairs, median candidate throughput delta is -1.9% and median latency delta is -2.3%.

Three sequential pairs on one host describe observed variation; they do not establish statistical significance. These transport-patched measurements do not approve the unpatched shipping SQLx dependency.
