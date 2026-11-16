//! Callback-wait mode for the long-horizon scenario (`CALLBACK_JOBS=N`).
//!
//! Instance 0 enqueues N jobs on their own queue, `CALLBACK_START_AFTER_S`
//! (default 0) after startup. Each handler registers a callback and parks in
//! `JobContext::wait_for_callback`, holding its worker slot while awa polls
//! the callback state. Once every replica's waiting count has settled (all N
//! on one replica, or unchanged for 10 s), each replica waits
//! `CALLBACK_RESOLVE_AFTER_S` (default 60) and resolves
//! `CALLBACK_RESOLVE_FRACTION` (default 1.0) of the callbacks its own
//! handlers registered through `admin::resume_external`. The payload carries
//! the wall-clock instant of the resolve call, so a resumed handler measures
//! resolve call -> handler resumes, across processes too.
//!
//! Sampler metrics: `callback_waiting`, `callback_resolved_total`,
//! `callback_resolve_calls_total`, `callback_errors_total` and, once a
//! handler has resumed, `callback_resume_{p50,p99,max}_ms` over every
//! resolution so far.

use async_trait::async_trait;
use awa_macros::JobArgs;
use awa_model::{insert, insert_many_copy_from_pool, InsertParams, QueueStorage};
use awa_worker::{JobContext, JobError, JobResult, QueueConfig, Worker};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

use crate::diagnostics::HANDLER_RETURNS;

pub const CALLBACK_QUEUE: &str = "awa_callback_bench";
const RESOLVE_CONCURRENCY: usize = 8;
const SETTLED_AFTER: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize, JobArgs)]
pub struct CallbackJob {
    pub seq: i64,
}

fn env_f64(name: &str) -> Option<f64> {
    std::env::var(name).ok().and_then(|raw| raw.parse().ok())
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct CallbackWaits {
    pub jobs: u32,
    start_after: Duration,
    resolve_after: Duration,
    resolve_fraction: f64,
    timeout: Duration,
    registered: Mutex<Vec<Uuid>>,
    waiting: AtomicI64,
    resolved: AtomicU64,
    resolve_calls: AtomicU64,
    errors: AtomicU64,
    resume_latencies_ms: Mutex<Vec<f64>>,
}

impl CallbackWaits {
    pub fn from_env() -> Option<Arc<Self>> {
        let jobs = std::env::var("CALLBACK_JOBS")
            .ok()
            .and_then(|raw| raw.parse::<u32>().ok())
            .filter(|jobs| *jobs > 0)?;
        let seconds = |name: &str, default: f64| {
            Duration::from_secs_f64(env_f64(name).unwrap_or(default).max(0.0))
        };
        Some(Arc::new(Self {
            jobs,
            start_after: seconds("CALLBACK_START_AFTER_S", 0.0),
            resolve_after: seconds("CALLBACK_RESOLVE_AFTER_S", 60.0),
            resolve_fraction: env_f64("CALLBACK_RESOLVE_FRACTION")
                .unwrap_or(1.0)
                .clamp(0.0, 1.0),
            timeout: seconds("CALLBACK_TIMEOUT_S", 3600.0),
            registered: Mutex::new(Vec::with_capacity(jobs as usize)),
            waiting: AtomicI64::new(0),
            resolved: AtomicU64::new(0),
            resolve_calls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            resume_latencies_ms: Mutex::new(Vec::with_capacity(jobs as usize)),
        }))
    }

    /// The callback queue: one worker slot per callback job, since a waiting
    /// handler keeps its slot.
    pub fn queue_config(&self, base: QueueConfig) -> QueueConfig {
        QueueConfig {
            max_workers: self.jobs,
            ..base
        }
    }

    pub fn worker(self: &Arc<Self>) -> CallbackWorker {
        CallbackWorker {
            waits: Arc::clone(self),
        }
    }

    /// Instance 0 enqueues the callback jobs after `CALLBACK_START_AFTER_S`.
    pub fn spawn_enqueue(
        self: &Arc<Self>,
        pool: PgPool,
        store: Option<QueueStorage>,
        shutdown: Arc<AtomicBool>,
    ) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            tokio::time::sleep(this.start_after).await;
            let params: Vec<InsertParams> = (0..i64::from(this.jobs))
                .map(|seq| {
                    insert::params_with(
                        &CallbackJob { seq },
                        awa_model::InsertOpts {
                            queue: CALLBACK_QUEUE.into(),
                            ..Default::default()
                        },
                    )
                    .expect("failed to build callback job params")
                })
                .collect();
            for chunk in params.chunks(500) {
                loop {
                    if shutdown.load(Ordering::Relaxed) {
                        return;
                    }
                    let res = match &store {
                        Some(store) => store.enqueue_params_copy(&pool, chunk).await,
                        None => insert_many_copy_from_pool(&pool, chunk)
                            .await
                            .map(|rows| rows.len()),
                    };
                    match res {
                        Ok(_) => break,
                        Err(err) => {
                            eprintln!("[awa] callback enqueue failed: {err:?}");
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            }
            eprintln!("[awa] callback: enqueued {} jobs", this.jobs);
        });
    }

    /// Every replica resolves the callbacks its own handlers registered.
    pub fn spawn_resolver(self: &Arc<Self>, pool: PgPool, shutdown: Arc<AtomicBool>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            let mut last_count = 0_i64;
            let mut last_change = Instant::now();
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
                let count = this.waiting.load(Ordering::Relaxed);
                if count != last_count {
                    last_count = count;
                    last_change = Instant::now();
                }
                if count >= i64::from(this.jobs)
                    || (count > 0 && last_change.elapsed() >= SETTLED_AFTER)
                {
                    break;
                }
            }
            eprintln!(
                "[awa] callback: {last_count} handlers waiting; resolving in {:.0}s",
                this.resolve_after.as_secs_f64()
            );
            tokio::time::sleep(this.resolve_after).await;
            let ids: Vec<Uuid> = {
                let registered = this.registered.lock().unwrap();
                let take = (registered.len() as f64 * this.resolve_fraction).ceil() as usize;
                registered.iter().take(take).copied().collect()
            };
            let ids = Arc::new(Mutex::new(ids));
            let mut tasks = Vec::with_capacity(RESOLVE_CONCURRENCY);
            for _ in 0..RESOLVE_CONCURRENCY {
                let this = Arc::clone(&this);
                let ids = Arc::clone(&ids);
                let pool = pool.clone();
                tasks.push(tokio::spawn(async move {
                    loop {
                        let Some(callback_id) = ids.lock().unwrap().pop() else {
                            return;
                        };
                        let payload = serde_json::json!({ "resolved_at_ms": now_epoch_ms() });
                        this.resolve_calls.fetch_add(1, Ordering::Relaxed);
                        if let Err(err) = awa_model::admin::resume_external(
                            &pool,
                            callback_id,
                            Some(payload),
                            None,
                        )
                        .await
                        {
                            this.errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[awa] callback resolve {callback_id} failed: {err}");
                        }
                    }
                }));
            }
            for task in tasks {
                let _ = task.await;
            }
            eprintln!(
                "[awa] callback: {} resolve calls issued",
                this.resolve_calls.load(Ordering::Relaxed)
            );
        });
    }

    /// Sampler metrics as (name, value).
    pub fn metrics(&self) -> Vec<(&'static str, f64)> {
        let mut out = vec![
            (
                "callback_waiting",
                self.waiting.load(Ordering::Relaxed).max(0) as f64,
            ),
            (
                "callback_resolved_total",
                self.resolved.load(Ordering::Relaxed) as f64,
            ),
            (
                "callback_resolve_calls_total",
                self.resolve_calls.load(Ordering::Relaxed) as f64,
            ),
            (
                "callback_errors_total",
                self.errors.load(Ordering::Relaxed) as f64,
            ),
        ];
        let mut latencies = self.resume_latencies_ms.lock().unwrap().clone();
        if let Some(&max) = latencies.iter().max_by(|a, b| a.total_cmp(b)) {
            latencies.sort_by(f64::total_cmp);
            let quantile = |q: f64| {
                latencies
                    [((q * (latencies.len() - 1) as f64).round() as usize).min(latencies.len() - 1)]
            };
            out.push(("callback_resume_p50_ms", quantile(0.50)));
            out.push(("callback_resume_p99_ms", quantile(0.99)));
            out.push(("callback_resume_max_ms", max));
        }
        out
    }
}

pub struct CallbackWorker {
    waits: Arc<CallbackWaits>,
}

#[async_trait]
impl Worker for CallbackWorker {
    fn kind(&self) -> &'static str {
        "bench_callback_job"
    }

    async fn perform(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let result = self.run(ctx).await;
        HANDLER_RETURNS.fetch_add(1, Ordering::Relaxed);
        result
    }
}

impl CallbackWorker {
    async fn run(&self, ctx: &JobContext) -> Result<JobResult, JobError> {
        let waits = &self.waits;
        let guard = ctx.register_callback(waits.timeout).await.map_err(|err| {
            waits.errors.fetch_add(1, Ordering::Relaxed);
            JobError::retryable(err)
        })?;
        waits.registered.lock().unwrap().push(guard.id());
        waits.waiting.fetch_add(1, Ordering::Relaxed);
        let outcome = ctx.wait_for_callback(guard).await;
        waits.waiting.fetch_sub(1, Ordering::Relaxed);
        match outcome {
            Ok(payload) => {
                if let Some(resolved_at_ms) = payload.get("resolved_at_ms").and_then(|v| v.as_i64())
                {
                    waits
                        .resume_latencies_ms
                        .lock()
                        .unwrap()
                        .push((now_epoch_ms() - resolved_at_ms).max(0) as f64);
                }
                waits.resolved.fetch_add(1, Ordering::Relaxed);
                Ok(JobResult::Completed)
            }
            Err(err) => {
                waits.errors.fetch_add(1, Ordering::Relaxed);
                Err(JobError::retryable(err))
            }
        }
    }
}
