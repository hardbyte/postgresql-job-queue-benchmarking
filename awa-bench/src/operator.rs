//! Operator scenarios: find jobs in an unconsumed haystack, and pause/resume
//! the bench queues. Contract: docs/operator-scenarios.md.
//!
//! Default-off: nothing runs unless `OPERATOR_CONTROL_FILE` is set. Every
//! replica watches the file and counts handler starts relative to the latest
//! op it saw; instance 0 performs each op on its own small pool and merges
//! the outcome into `OPERATOR_RESULT_FILE`.
//!
//! The lane ops (`lane_cancel`, `lane_move`, `lane_reprioritise`) act on the
//! haystack this process enqueued, which pins awa's queue ring while no
//! worker claims it: `admin::bulk_cancel` on its ids, or a batch operation
//! (`MoveQueue` to a consumed queue, `SetPriority`) over its queue, waited on
//! until the maintenance leader finishes it. Moved jobs reach the bench
//! worker, which completes them and counts them (`haystack_completed_total`).
//!
//! The probes use awa's public admin API. The default build targets 0.6
//! (`get_job`, `list_jobs`, `queue_overview`); the `awa07` feature targets
//! 0.7 (`get_job`, `find_jobs`, `queue_overview`, and the tag-index build after
//! the haystack).

use awa_model::batch_operations::{
    get_batch_operation, submit_batch_operation, BatchOperationFilter, BatchOperationSpec,
    BatchOperationState, SubmitBatchOperation,
};
use awa_model::{
    admin, insert_many_copy_from_pool, AwaError, InsertOpts, InsertParams, JobRow, JobState,
};
use serde_json::{json, Map, Value};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const OPERATOR_POOL_CONNECTIONS: u32 = 4;
const HAYSTACK_BATCH: i64 = 1000;
const KEPT_PAIRS: usize = 10_000;
const PRIMARY_KIND: &str = "long_horizon_job";
const ALT_KIND: &str = "bench_alt";
const PAUSED_BY: &str = "bench-operator";
const PROBES: [&str; 5] = [
    "get_by_id",
    "find_handle",
    "find_tenant_page",
    "kind_state_page",
    "count_state",
];

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Handler starts in this process, relative to the latest op it saw.
#[derive(Default)]
pub struct JobStarts {
    total: AtomicU64,
    last_ms: AtomicI64,
    first_after_op_ms: AtomicI64,
    op_seen_ms: AtomicI64,
}

impl JobStarts {
    pub fn record(&self) {
        let now = now_ms();
        self.total.fetch_add(1, Ordering::Relaxed);
        self.last_ms.store(now, Ordering::Relaxed);
        if self.op_seen_ms.load(Ordering::Relaxed) > 0 {
            let _ = self.first_after_op_ms.compare_exchange(
                0,
                now,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    fn op_seen(&self) {
        self.first_after_op_ms.store(0, Ordering::Relaxed);
        self.op_seen_ms.store(now_ms(), Ordering::Relaxed);
    }

    pub fn metrics(&self) -> [(&'static str, f64); 3] {
        [
            (
                "job_starts_total",
                self.total.load(Ordering::Relaxed) as f64,
            ),
            (
                "last_job_start_ms",
                self.last_ms.load(Ordering::Relaxed) as f64,
            ),
            (
                "first_start_after_op_ms",
                self.first_after_op_ms.load(Ordering::Relaxed) as f64,
            ),
        ]
    }
}

/// What the haystack op enqueued, for the probes' ground truth.
struct Haystack {
    queue: String,
    jobs: i64,
    tenants: i64,
    alt_every: i64,
    /// Uniform sample of `(job id, seq)`.
    pairs: Vec<(i64, i64)>,
    /// Every id, for `lane_cancel`.
    ids: Vec<i64>,
}

impl Haystack {
    fn tenant_matches(&self, tenant: i64) -> i64 {
        self.jobs / self.tenants + i64::from(tenant < self.jobs % self.tenants)
    }

    fn alt_matches(&self) -> i64 {
        if self.alt_every == 0 {
            return 0;
        }
        (self.jobs + self.alt_every - 1) / self.alt_every
    }
}

struct Command {
    id: String,
    op: String,
    args: Map<String, Value>,
}

fn read_command(path: &str) -> Option<Command> {
    let raw = std::fs::read_to_string(path).ok()?;
    let value: Value = serde_json::from_str(&raw).ok()?;
    Some(Command {
        id: value.get("id")?.as_str()?.to_owned(),
        op: value.get("op")?.as_str()?.to_owned(),
        args: value
            .get("args")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default(),
    })
}

fn arg_i64(args: &Map<String, Value>, key: &str, default: i64) -> i64 {
    args.get(key)
        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
        .unwrap_or(default)
}

fn arg_f64(args: &Map<String, Value>, key: &str, default: f64) -> f64 {
    args.get(key).and_then(Value::as_f64).unwrap_or(default)
}

fn arg_str(args: &Map<String, Value>, key: &str) -> Option<String> {
    args.get(key).and_then(Value::as_str).map(str::to_owned)
}

/// splitmix64: enough randomness to pick probe targets and sample ids.
struct Rng(u64);

impl Rng {
    fn seeded() -> Self {
        Self(now_ms() as u64 ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

pub struct Operator {
    control_path: String,
    result_path: Option<String>,
    database_url: String,
    bench_queues: Vec<String>,
    starts: Arc<JobStarts>,
    pool: OnceCell<PgPool>,
    haystack: Mutex<Option<Arc<Haystack>>>,
    result_lock: tokio::sync::Mutex<()>,
}

impl Operator {
    pub fn from_env(database_url: &str, bench_queues: &[String]) -> Option<Arc<Self>> {
        let control_path = std::env::var("OPERATOR_CONTROL_FILE")
            .ok()
            .filter(|p| !p.is_empty())?;
        Some(Arc::new(Self {
            control_path,
            result_path: std::env::var("OPERATOR_RESULT_FILE")
                .ok()
                .filter(|p| !p.is_empty()),
            database_url: database_url.to_owned(),
            bench_queues: bench_queues.to_vec(),
            starts: Arc::new(JobStarts::default()),
            pool: OnceCell::new(),
            haystack: Mutex::new(None),
            result_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn starts(&self) -> Arc<JobStarts> {
        Arc::clone(&self.starts)
    }

    /// Watch the control file; `leader` (instance 0) performs each new op.
    /// The id present at startup is adopted as already seen.
    pub fn spawn(self: &Arc<Self>, leader: bool) {
        let this = Arc::clone(self);
        let mut seen = read_command(&this.control_path).map(|c| c.id);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(POLL_INTERVAL).await;
                let Some(command) = read_command(&this.control_path) else {
                    continue;
                };
                if seen.as_deref() == Some(command.id.as_str()) {
                    continue;
                }
                seen = Some(command.id.clone());
                this.starts.op_seen();
                if leader {
                    let worker = Arc::clone(&this);
                    tokio::spawn(async move { worker.perform(command).await });
                }
            }
        });
    }

    async fn pool(&self) -> Result<&PgPool, String> {
        self.pool
            .get_or_try_init(|| async {
                PgPoolOptions::new()
                    .max_connections(OPERATOR_POOL_CONNECTIONS)
                    .connect(&self.database_url)
                    .await
            })
            .await
            .map_err(|err| format!("operator pool: {err}"))
    }

    async fn perform(&self, command: Command) {
        eprintln!("[awa] operator {} ({})", command.op, command.id);
        let result = match command.op.as_str() {
            "haystack" => self.haystack(&command.args).await,
            "probe" => self.probe(&command.args).await,
            "pause" => self.pause_or_resume(&command.args, true).await,
            "resume" => self.pause_or_resume(&command.args, false).await,
            "lane_cancel" => self.lane_cancel().await,
            "lane_move" => {
                let to =
                    arg_str(&command.args, "to").unwrap_or_else(|| self.bench_queues[0].clone());
                let spec = BatchOperationSpec::MoveQueue {
                    queue: to,
                    priority: None,
                };
                self.lane_batch(&command.args, spec).await
            }
            "lane_reprioritise" => {
                let priority = arg_i64(&command.args, "priority", 3) as i16;
                self.lane_batch(&command.args, BatchOperationSpec::SetPriority { priority })
                    .await
            }
            other => Err(format!("unknown op {other:?}")),
        };
        let mut outcome = match result {
            Ok(fields) => fields,
            Err(error) => {
                eprintln!("[awa] operator {} failed: {error}", command.op);
                let mut fields = Map::new();
                fields.insert("error".into(), json!(error));
                fields
            }
        };
        outcome.insert("done".into(), json!(true));
        outcome.entry("done_ms").or_insert(json!(now_ms()));
        if let Err(err) = self.write_result(&command.id, Value::Object(outcome)).await {
            eprintln!("[awa] operator result write failed: {err}");
        }
    }

    async fn write_result(&self, id: &str, outcome: Value) -> std::io::Result<()> {
        let Some(path) = &self.result_path else {
            return Ok(());
        };
        let _guard = self.result_lock.lock().await;
        let mut all: Map<String, Value> = std::fs::read_to_string(path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok())
            .unwrap_or_default();
        all.insert(id.to_owned(), outcome);
        let tmp = format!("{path}.tmp-{}", std::process::id());
        std::fs::write(&tmp, serde_json::to_vec(&all)?)?;
        std::fs::rename(&tmp, path)
    }

    async fn haystack(&self, args: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let jobs = arg_i64(args, "jobs", 0).max(0);
        let queue = arg_str(args, "queue").unwrap_or_else(|| "haystack".into());
        let tenants = arg_i64(args, "tenants", 1000).max(1);
        // alt_every=0: every job gets the bench kind, so a lane op can move
        // the whole haystack onto a consumed queue.
        let alt_every = arg_i64(args, "alt_every", 20).max(0);
        let pool = self.pool().await?;

        // `QueueStorage::enqueue_params_copy` returns only a count, so the
        // haystack goes through the COPY insert that returns rows: the
        // probes need ids.
        let started = Instant::now();
        let mut rng = Rng::seeded();
        let mut pairs: Vec<(i64, i64)> = Vec::with_capacity(KEPT_PAIRS);
        let mut ids: Vec<i64> = Vec::with_capacity(jobs as usize);
        let mut offered: usize = 0;
        let mut enqueued: i64 = 0;
        let mut seq = 0;
        while seq < jobs {
            let batch_end = (seq + HAYSTACK_BATCH).min(jobs);
            let params: Vec<InsertParams> = (seq..batch_end)
                .map(|s| haystack_params(s, &queue, tenants, alt_every))
                .collect();
            let rows = insert_many_copy_from_pool(pool, &params)
                .await
                .map_err(|err| format!("haystack insert at seq {seq}: {err}"))?;
            enqueued += rows.len() as i64;
            for row in rows {
                ids.push(row.id);
                let Some(job_seq) = row.args.get("seq").and_then(Value::as_i64) else {
                    continue;
                };
                // Reservoir sampling keeps a uniform sample of every id.
                if pairs.len() < KEPT_PAIRS {
                    pairs.push((row.id, job_seq));
                } else {
                    let slot = rng.below(offered + 1);
                    if slot < KEPT_PAIRS {
                        pairs[slot] = (row.id, job_seq);
                    }
                }
                offered += 1;
            }
            seq = batch_end;
        }
        let seconds = started.elapsed().as_secs_f64();
        *self.haystack.lock().unwrap() = Some(Arc::new(Haystack {
            queue,
            jobs,
            tenants,
            alt_every,
            pairs,
            ids,
        }));
        let mut out = Map::new();
        out.insert("enqueued".into(), json!(enqueued));
        out.insert("seconds".into(), json!(seconds));
        api::after_haystack(pool, &mut out).await?;
        Ok(out)
    }

    async fn probe(&self, args: &Map<String, Value>) -> Result<Map<String, Value>, String> {
        let haystack = self
            .haystack
            .lock()
            .unwrap()
            .clone()
            .filter(|h| !h.pairs.is_empty())
            .ok_or("no haystack enqueued by this process")?;
        let duration = Duration::from_secs_f64(arg_f64(args, "duration_s", 60.0).max(0.0));
        let rate = arg_f64(args, "rate", 2.0).max(0.01);
        let page = arg_i64(args, "page", 100).max(1);
        let timeout = Duration::from_millis(arg_i64(args, "timeout_ms", 30_000).max(1) as u64);
        let queue = arg_str(args, "queue").unwrap_or_else(|| haystack.queue.clone());
        let pool = self.pool().await?;

        // Each probe runs on its own timeline at rate / 5 per second,
        // staggered so the five interleave, so one probe that runs to the
        // timeout doesn't starve the others.
        let slot = Duration::from_secs_f64(1.0 / rate);
        let interval = slot * PROBES.len() as u32;
        let start = Instant::now();
        let end = start + duration;
        let mut loops = tokio::task::JoinSet::new();
        for (probe, name) in PROBES.iter().enumerate() {
            let pool = pool.clone();
            let haystack = Arc::clone(&haystack);
            let queue = queue.clone();
            loops.spawn(async move {
                let mut stat = ProbeStats::default();
                let mut rng = Rng(Rng::seeded().0 ^ (probe as u64 + 1));
                let mut next = start + slot * probe as u32;
                while next < end {
                    tokio::time::sleep_until(next.into()).await;
                    let target = haystack.pairs[rng.below(haystack.pairs.len())];
                    let started = Instant::now();
                    let outcome = tokio::time::timeout(
                        timeout,
                        run_probe(&pool, probe, target, &haystack, &queue, page),
                    )
                    .await;
                    stat.latencies_ms
                        .push(started.elapsed().as_secs_f64() * 1000.0);
                    match outcome {
                        Err(_) => stat.timeouts += 1,
                        Ok(Err(err)) => {
                            stat.errors += 1;
                            eprintln!("[awa] probe {name} failed: {err}");
                        }
                        Ok(Ok(checked)) => {
                            stat.rows.push(checked.rows);
                            stat.correct += u64::from(checked.correct);
                        }
                    }
                    next = (next + interval).max(Instant::now());
                }
                (probe, stat)
            });
        }
        let mut stats: Vec<ProbeStats> = PROBES.iter().map(|_| ProbeStats::default()).collect();
        while let Some(joined) = loops.join_next().await {
            let (probe, stat) = joined.map_err(|err| format!("probe task: {err}"))?;
            stats[probe] = stat;
        }
        let probes: Map<String, Value> = PROBES
            .iter()
            .zip(stats)
            .map(|(name, stat)| ((*name).to_owned(), stat.summary()))
            .collect();
        let mut out = Map::new();
        out.insert("probes".into(), Value::Object(probes));
        out.insert("done_ms".into(), json!(now_ms()));
        Ok(out)
    }

    /// Cancel every job of the haystack through `admin::bulk_cancel`.
    async fn lane_cancel(&self) -> Result<Map<String, Value>, String> {
        let haystack = self
            .haystack
            .lock()
            .unwrap()
            .clone()
            .ok_or("no haystack enqueued by this process")?;
        let pool = self.pool().await?;
        let started = Instant::now();
        let mut cancelled = 0_usize;
        for chunk in haystack.ids.chunks(HAYSTACK_BATCH as usize) {
            cancelled += admin::bulk_cancel(pool, chunk)
                .await
                .map_err(|err| format!("bulk_cancel: {err}"))?
                .len();
        }
        let mut out = Map::new();
        out.insert("mode".into(), json!("native"));
        out.insert("cancelled".into(), json!(cancelled));
        out.insert("requested".into(), json!(haystack.ids.len()));
        out.insert("api_s".into(), json!(started.elapsed().as_secs_f64()));
        out.insert("done_ms".into(), json!(now_ms()));
        Ok(out)
    }

    /// Submit a batch operation over the haystack queue's available jobs
    /// and wait for the maintenance leader to finish it.
    async fn lane_batch(
        &self,
        args: &Map<String, Value>,
        spec: BatchOperationSpec,
    ) -> Result<Map<String, Value>, String> {
        let queue = arg_str(args, "queue").unwrap_or_else(|| "haystack".into());
        let pool = self.pool().await?;
        let started = Instant::now();
        let submitted = submit_batch_operation(
            pool,
            SubmitBatchOperation {
                spec,
                filter: BatchOperationFilter {
                    queue: Some(queue),
                    state: Some(JobState::Available),
                    ..Default::default()
                },
                submitted_by: Some(PAUSED_BY.into()),
                allow_all: false,
            },
        )
        .await
        .map_err(|err| format!("submit_batch_operation: {err}"))?;
        let submit_ms = started.elapsed().as_secs_f64() * 1000.0;
        let operation = loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let operation = get_batch_operation(pool, submitted.id)
                .await
                .map_err(|err| format!("get_batch_operation: {err}"))?;
            if matches!(
                operation.state,
                BatchOperationState::Completed
                    | BatchOperationState::Cancelled
                    | BatchOperationState::Failed
            ) {
                break operation;
            }
        };
        let mut out = Map::new();
        out.insert("mode".into(), json!("native"));
        out.insert(
            "completed".into(),
            json!(operation.state == BatchOperationState::Completed),
        );
        out.insert("matched".into(), json!(operation.total_matched));
        out.insert("processed".into(), json!(operation.processed));
        out.insert("skipped".into(), json!(operation.skipped));
        out.insert("errored".into(), json!(operation.errored));
        out.insert("submit_ms".into(), json!(submit_ms));
        out.insert("api_s".into(), json!(started.elapsed().as_secs_f64()));
        out.insert("done_ms".into(), json!(now_ms()));
        if let Some(error) = operation.last_error {
            out.insert("error".into(), json!(error));
        }
        Ok(out)
    }

    async fn pause_or_resume(
        &self,
        args: &Map<String, Value>,
        pause: bool,
    ) -> Result<Map<String, Value>, String> {
        let queues = arg_str(args, "queue")
            .map(|q| vec![q])
            .unwrap_or_else(|| self.bench_queues.clone());
        let pool = self.pool().await?;
        let started = Instant::now();
        for queue in &queues {
            let result = if pause {
                admin::pause_queue(pool, queue, Some(PAUSED_BY)).await
            } else {
                admin::resume_queue(pool, queue).await
            };
            result.map_err(|err| format!("{queue}: {err}"))?;
        }
        let mut out = Map::new();
        out.insert("mode".into(), json!("native"));
        out.insert(
            "api_ms".into(),
            json!(started.elapsed().as_secs_f64() * 1000.0),
        );
        out.insert("done_ms".into(), json!(now_ms()));
        Ok(out)
    }
}

fn haystack_params(seq: i64, queue: &str, tenants: i64, alt_every: i64) -> InsertParams {
    InsertParams {
        kind: if alt_every > 0 && seq % alt_every == 0 {
            ALT_KIND
        } else {
            PRIMARY_KIND
        }
        .to_owned(),
        args: json!({ "seq": seq }),
        opts: InsertOpts {
            queue: queue.to_owned(),
            tags: vec![format!("h{seq}"), format!("t{}", seq % tenants)],
            ..Default::default()
        },
    }
}

struct Checked {
    rows: i64,
    correct: bool,
}

async fn run_probe(
    pool: &PgPool,
    probe: usize,
    (job_id, seq): (i64, i64),
    haystack: &Haystack,
    queue: &str,
    page: i64,
) -> Result<Checked, AwaError> {
    let handle = format!("h{seq}");
    let has_tag = |row: &JobRow, tag: &str| row.tags.iter().any(|t| t == tag);
    match PROBES[probe] {
        "get_by_id" => match admin::get_job(pool, job_id).await {
            Ok(row) => Ok(Checked {
                rows: 1,
                correct: has_tag(&row, &handle),
            }),
            Err(AwaError::JobNotFound { .. }) => Ok(Checked {
                rows: 0,
                correct: false,
            }),
            Err(err) => Err(err),
        },
        "find_handle" => {
            let rows = api::find_by_tag(pool, &handle, 2).await?;
            Ok(Checked {
                rows: rows.len() as i64,
                correct: rows.len() == 1 && rows[0].id == job_id,
            })
        }
        "find_tenant_page" => {
            let tenant = seq % haystack.tenants;
            let tag = format!("t{tenant}");
            let rows = api::find_by_tag(pool, &tag, page).await?;
            let expected = page.min(haystack.tenant_matches(tenant));
            Ok(Checked {
                rows: rows.len() as i64,
                correct: rows.len() as i64 == expected && rows.iter().all(|r| has_tag(r, &tag)),
            })
        }
        "kind_state_page" => {
            let rows = api::available_kind_page(pool, queue, ALT_KIND, page).await?;
            let expected = page.min(haystack.alt_matches());
            Ok(Checked {
                rows: rows.len() as i64,
                correct: rows.len() as i64 == expected
                    && rows.iter().all(|r| r.kind == ALT_KIND && r.queue == queue),
            })
        }
        _ => {
            let count = api::count_available(pool, queue).await?;
            Ok(Checked {
                rows: count,
                correct: (count - haystack.jobs).abs() as f64 <= 0.01 * haystack.jobs as f64,
            })
        }
    }
}

#[derive(Default)]
struct ProbeStats {
    latencies_ms: Vec<f64>,
    rows: Vec<i64>,
    correct: u64,
    errors: u64,
    timeouts: u64,
}

impl ProbeStats {
    fn summary(mut self) -> Value {
        let n = self.latencies_ms.len();
        if n == 0 {
            return json!({ "mode": "native", "n": 0 });
        }
        self.latencies_ms.sort_by(f64::total_cmp);
        self.rows.sort_unstable();
        let at = |q: f64| self.latencies_ms[((q * (n - 1) as f64).round() as usize).min(n - 1)];
        let rows_median = if self.rows.is_empty() {
            Value::Null
        } else {
            json!(self.rows[self.rows.len() / 2])
        };
        json!({
            "mode": "native",
            "n": n,
            "p50_ms": at(0.50),
            "p99_ms": at(0.99),
            "max_ms": self.latencies_ms[n - 1],
            "rows_median": rows_median,
            "correct": self.correct as f64 / n as f64,
            "errors": self.errors,
            "timeouts": self.timeouts,
        })
    }
}

/// awa 0.6: `list_jobs` takes one tag and checks it in Rust after loading
/// candidates (no tag index); counts come from `queue_overview`.
#[cfg(not(feature = "awa07"))]
mod api {
    use awa_model::{admin, AwaError, JobRow, JobState, ListJobsFilter};
    use serde_json::{Map, Value};
    use sqlx::PgPool;

    pub async fn find_by_tag(
        pool: &PgPool,
        tag: &str,
        limit: i64,
    ) -> Result<Vec<JobRow>, AwaError> {
        let filter = ListJobsFilter {
            tag: Some(tag.to_owned()),
            limit: Some(limit),
            ..Default::default()
        };
        admin::list_jobs(pool, &filter).await
    }

    pub async fn available_kind_page(
        pool: &PgPool,
        queue: &str,
        kind: &str,
        limit: i64,
    ) -> Result<Vec<JobRow>, AwaError> {
        let filter = ListJobsFilter {
            state: Some(JobState::Available),
            kind: Some(kind.to_owned()),
            queue: Some(queue.to_owned()),
            limit: Some(limit),
            ..Default::default()
        };
        admin::list_jobs(pool, &filter).await
    }

    pub async fn count_available(pool: &PgPool, queue: &str) -> Result<i64, AwaError> {
        Ok(admin::queue_overview(pool, queue)
            .await?
            .map_or(0, |overview| overview.available))
    }

    pub async fn after_haystack(
        _pool: &PgPool,
        _out: &mut Map<String, Value>,
    ) -> Result<(), String> {
        Ok(())
    }
}

/// awa 0.7 (ADR-049): `find_jobs`/`count_jobs` over a `JobFilter`, served by
/// the per-partition tag GIN index. (`count_jobs` is exact only up to
/// `COUNT_CAP` (10,000), so counts use `queue_overview`.)
#[cfg(feature = "awa07")]
mod api {
    use awa_model::tag_index::{build_tag_indexes, BuildMode};
    use awa_model::{admin, AwaError, JobFilter, JobPage, JobRow, JobState};
    use serde_json::{json, Map, Value};
    use sqlx::PgPool;
    use std::time::Instant;

    pub async fn find_by_tag(
        pool: &PgPool,
        tag: &str,
        limit: i64,
    ) -> Result<Vec<JobRow>, AwaError> {
        admin::find_jobs(pool, &JobFilter::new().tag(tag), JobPage::first(limit)).await
    }

    pub async fn available_kind_page(
        pool: &PgPool,
        queue: &str,
        kind: &str,
        limit: i64,
    ) -> Result<Vec<JobRow>, AwaError> {
        let filter = JobFilter::new()
            .kind(kind)
            .queue(queue)
            .state(JobState::Available);
        admin::find_jobs(pool, &filter, JobPage::first(limit)).await
    }

    /// An exact per-queue count comes from `queue_overview`, as on 0.6;
    /// `count_jobs` stops at 10,000 by design and reports "10,000+".
    pub async fn count_available(pool: &PgPool, queue: &str) -> Result<i64, AwaError> {
        Ok(admin::queue_overview(pool, queue)
            .await?
            .map_or(0, |overview| overview.available))
    }

    /// The operator step `awa storage build-tag-index` (one pass), as its
    /// library call. Fresh schemas come out indexed, so this normally finds
    /// every index valid.
    pub async fn after_haystack(pool: &PgPool, out: &mut Map<String, Value>) -> Result<(), String> {
        let started = Instant::now();
        let pass = build_tag_indexes(pool, BuildMode::WhenEmpty)
            .await
            .map_err(|err| format!("build_tag_indexes: {err}"))?;
        out.insert("tag_index_s".into(), json!(started.elapsed().as_secs_f64()));
        out.insert("tag_index_built".into(), json!(pass.built));
        out.insert("tag_index_repaired".into(), json!(pass.repaired));
        out.insert("tag_index_already_valid".into(), json!(pass.already_valid));
        out.insert("tag_index_waiting".into(), json!(pass.waiting.len()));
        Ok(())
    }
}
