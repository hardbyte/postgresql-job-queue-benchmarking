# Operator scenarios: adapter contract

Two scenarios measure operator work that a production queue has to get right:

- **Find a job (needle in a haystack).** A million jobs sit in a queue no worker consumes, while ordinary load runs on the bench queue. The operator looks up one job by id, filters for a single job by a unique handle, pages through a tenant's jobs, pages by kind and state, and counts by state.
- **Pause and resume a queue.** Two or more worker processes run under steady load. The operator pauses the bench queue, starts a fresh worker process while it is paused, and then resumes the queue.

Each result is labelled with how it was obtained:

- `native` means the library's public API was used.
- `sql` means raw SQL was used, where that is the documented interface or the only way.
- `emulated` means a pause that the harness reproduces by stopping each process locally.
- `unsupported` means no number is reported.

Use the best public API each library documents. Don't add indexes or schema the library doesn't create itself.

## Harness side

An `operator(op=<name>, k=v, …)` phase writes this to `OPERATOR_CONTROL_FILE`:

```json
{"id": "<phase label>-<issued_ms>", "op": "haystack|probe|pause|resume", "args": {"…": "…", "duration_s": 180}, "issued_ms": 1790000000000}
```

The file starts as `{}`. Writes are atomic renames. Integer and float args arrive as numbers.

At phase exit the harness waits for instance 0 to report the op's outcome in `OPERATOR_RESULT_FILE`. It waits up to the phase's `wait=` param, default 30 min, so a haystack may outlast its phase. It records the outcome's numeric fields as samples and builds `systems.<s>.operator` in `summary.json` (see `writers._operator_summary`).

## Every replica

- Poll `OPERATOR_CONTROL_FILE` at least every 100 ms when the variable is set. Do nothing when it is unset; other scenarios must be unchanged.
- At startup, adopt the file's current `id` as already seen without acting on it. A process started mid-pause behaves like any restarted process: it honours a pause only if the system stores pause state.
- On a new `id`, note the time the op was seen.
- Emit three adapter metrics on every sample tick, alongside the existing ones:
  - `job_starts_total`: cumulative number of handler starts on the bench queue(s) in this process.
  - `last_job_start_ms`: epoch ms of the most recent handler start, or 0.
  - `first_start_after_op_ms`: epoch ms of the first handler start after this replica saw the latest op, or 0 until one happens.
- For `pause` and `resume` in `emulated` mode (no native pause), every replica stops or starts claiming locally when it sees the op. This applies only to systems without a pause API.

## Instance 0

Instance 0 performs the op and merges `{"<id>": {...outcome..., "done": true}}` into `OPERATOR_RESULT_FILE`. Read the existing JSON, add the key, then write it back with an atomic rename. Every outcome carries `done_ms`: the epoch ms when the call or work finished. On failure it carries `"error": "<message>"`, plus `done` as usual.

### `haystack` — args `{jobs, queue="haystack", tenants=1000, alt_every=20}`

Enqueue `jobs` jobs into `queue` through the adapter's bulk insert path. No worker may consume that queue: no worker subscribes to it, and for awa no queue config exists for it.

Job `seq = 0..jobs-1` carries:

| Handle | Value | Matches |
|---|---|---|
| unique handle | `h<seq>` | 1 job |
| tenant | `t<seq % tenants>` | `jobs / tenants` jobs |
| kind | `bench_alt` when `seq % alt_every == 0`, otherwise the usual kind | 1 job in `alt_every` |

Put the handles where the system's filter API can reach them:
- awa: tags `["h<seq>", "t<k>"]`, and kind via the job kind.
- River: `metadata` `{"h": "h<seq>", "t": "t<k>"}` (GIN `@>`), and kind via the job kind.
- Oban: `args` `{"h": …, "t": …}` (GIN), and kind via the worker.
- pg-boss: `data` `{h, t}`.
- Procrastinate: task args `{h, t}`, and kind via the task name.
- Absurd: params `{h, t}`, and kind via the task name.
- pgmq: message `{h, t}`.
- pgque: event data, plus `ev_type` for kind.

Keep up to 10,000 `(job id, seq)` pairs, sampled uniformly, for the probes.

The outcome is `{enqueued, seconds, done_ms}`.

### `probe` — args `{duration_s, rate=2, queue="haystack", page=100, timeout_ms=30000}`

For `duration_s`, run the probes below in rotation at `rate` per second total, from a dedicated connection or client so that probes don't take worker connections. Pick each probe's target at random from the kept pairs.

| Probe | What it does | Correct if |
|---|---|---|
| `get_by_id` | fetch one job by id | the job is found and its handle matches |
| `find_handle` | filter by the unique handle (`h<seq>`), limit 2 | exactly that one job |
| `find_tenant_page` | filter by tenant `t<k>`, first page of `page` | `page` rows, or `min(page, matches)` |
| `kind_state_page` | kind `bench_alt`, available/queued state, in `queue`, first page of `page` | `page` rows |
| `count_state` | count available/queued jobs in `queue` | about `jobs`, give or take 1% |

A probe that exceeds `timeout_ms` counts as a timeout. Cancel it if the API allows, then move on.

The outcome is `{probes: {<name>: {mode, n, p50_ms, p99_ms, max_ms, rows_median, correct, errors, timeouts}}, done_ms}`:
- `correct` is the fraction of probes that were correct.
- `mode` is `unsupported` when the library has no way to do it. Report `n: 0` and skip it.
- Report `sql` only where the library documents SQL as its interface (pgmq), or offers nothing else and the query is the obvious one against its own tables. Say which in a comment in the adapter.

### `pause` / `resume` — args `{queue=<bench queue>}`

Pause or resume every bench queue the adapter consumes, through the native API. The outcome is `{mode, api_ms, done_ms}`, where `api_ms` is the time the call took.

Under `emulated`, instance 0 still reports `done_ms` once its own local stop or start has happened.

## Capabilities

Add a `"operator_capabilities"` object to the adapter's `adapter.json`, mapping each probe name plus `pause` to the mode the adapter implements. Add a one-line `"operator_notes"` string naming the APIs used.
