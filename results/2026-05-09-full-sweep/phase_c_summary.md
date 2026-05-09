### Phase C — stress/clean completion-rate ratio

Higher = better. `(rc=137)` means the adapter hung in shutdown after the stress phase and the timeout fired before recovery samples were written.

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| idle_in_tx | 75% | (rc=137) | 96% | (rc=137) | (rc=137) | (rc=137) | 100% | (rc=137) |
| sustained_high_load | 73% | 71% | 102% | 136% | 94% | 140% | 100% | 150% |
| active_readers | 71% | 74% | 98% | 101% | 63% | 100% | 100% | 39% |
| event_burst | 80% | (rc=137) | 100% | (rc=137) | (rc=137) | (rc=137) | 100% | (rc=137) |

### Clean-phase completion rate (jobs/s)

| scenario | awa | absurd | oban | pgboss | procrastinate | river | pgque | pgmq |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| idle_in_tx | 2,851 | — | 228 | — | — | — | 799 | — |
| sustained_high_load | 3,691 | 198 | 224 | 754 | 254 | 232 | 799 | 802 |
| active_readers | 3,518 | 198 | 227 | 752 | 262 | 239 | 801 | 802 |
| event_burst | 6,189 | — | 227 | — | — | — | 1,197 | — |
