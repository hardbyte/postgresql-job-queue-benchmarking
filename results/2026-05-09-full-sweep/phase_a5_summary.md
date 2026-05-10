## Phase A.5 — attribution A/B cells

### awa LEASE_DEADLINE_MS=30000 attribution

| Worker count | headline (lib default) | A.5 (LEASE_DEADLINE_MS=30000) | delta % |
|---|---:|---:|---:|
| W=64 | 5,369 | 4,966 | -7.5% |
| W=128 | 8,499 | 8,390 | -1.3% |
| W=256 | 14,158 | 13,597 | -4.0% |

### pgque shared-mode attribution (W=64)

| Mode | completion_rate (jobs/s) |
|---|---:|
| subconsumer (headline) | 27,719 |
| shared (A.5) | 23,979 |
| delta | -13.5% |
