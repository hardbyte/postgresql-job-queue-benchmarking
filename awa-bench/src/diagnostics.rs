//! awa-only diagnostics for focused A/B runs.
//!
//! `MaintenanceCapture` keeps awa's own OpenTelemetry metrics in memory so the
//! sampler can report cumulative ring maintenance outcomes (`prune` and
//! `rotate` attempts by ring and outcome). Only this process's runtime is
//! covered: with several replicas, the counters live on whichever replica
//! leads maintenance.
//!
//! `PointReads` drives `admin::get_job` at `AWA_POINT_READS_PER_S` against
//! jobs this process has started: half recent ids, half older ids, so reads
//! land both on live slots and on slots that rotation has sealed.

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader};
use sqlx::PgPool;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAINTENANCE_METRICS: [&str; 2] = [
    "awa.maintenance.prune.attempts",
    "awa.maintenance.rotate.attempts",
];

pub struct MaintenanceCapture {
    exporter: InMemoryMetricExporter,
}

static CAPTURE: OnceLock<MaintenanceCapture> = OnceLock::new();

/// A reader to add to the process's meter provider; its exports are read
/// back by [`maintenance_totals`].
pub fn maintenance_reader(interval: Duration) -> PeriodicReader<InMemoryMetricExporter> {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone())
        .with_interval(interval)
        .build();
    let _ = CAPTURE.set(MaintenanceCapture { exporter });
    reader
}

fn attribute<'a>(
    attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>,
    key: &str,
) -> Option<String> {
    attributes
        .into_iter()
        .find(|kv| kv.key.as_str() == key)
        .map(|kv| kv.value.as_str().into_owned())
}

/// Latest cumulative maintenance counters as
/// `maint_<prune|rotate>_<ring>_<outcome>_total`, or empty before the
/// first export.
pub fn maintenance_totals() -> Vec<(String, f64)> {
    let Some(capture) = CAPTURE.get() else {
        return Vec::new();
    };
    let Ok(exports) = capture.exporter.get_finished_metrics() else {
        return Vec::new();
    };
    // Cumulative temporality: the last export holds the running totals.
    let mut totals: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(latest) = exports.last() {
        for scope in latest.scope_metrics() {
            for metric in scope.metrics() {
                let name = metric.name();
                if !MAINTENANCE_METRICS.contains(&name) {
                    continue;
                }
                let kind = if name.contains("prune") {
                    "prune"
                } else {
                    "rotate"
                };
                let mut add = |ring: Option<String>, outcome: Option<String>, value: f64| {
                    let key = format!(
                        "maint_{kind}_{}_{}_total",
                        ring.unwrap_or_else(|| "unknown".into()),
                        outcome.unwrap_or_else(|| "unknown".into())
                    );
                    *totals.entry(key).or_default() += value;
                };
                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            add(
                                attribute(point.attributes(), "awa.ring"),
                                attribute(point.attributes(), "awa.ring.outcome"),
                                point.value() as f64,
                            );
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            add(
                                attribute(point.attributes(), "awa.ring"),
                                attribute(point.attributes(), "awa.ring.outcome"),
                                point.value() as f64,
                            );
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            add(
                                attribute(point.attributes(), "awa.ring"),
                                attribute(point.attributes(), "awa.ring.outcome"),
                                point.value(),
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    }
    // Keep only the newest export so memory stays flat over long runs.
    capture.exporter.reset();
    totals.into_iter().collect()
}

const RECENT_CAPACITY: usize = 1024;
const OLDER_CAPACITY: usize = 16384;
const OLDER_EVERY: u64 = 16;

pub struct PointReads {
    recent: Mutex<VecDeque<i64>>,
    older: Mutex<VecDeque<i64>>,
    observed: AtomicU64,
    pub found: AtomicU64,
    pub not_found: AtomicU64,
    pub errors: AtomicU64,
    latencies_ms: Mutex<VecDeque<(Instant, f64)>>,
}

impl PointReads {
    pub fn from_env() -> Option<(Arc<Self>, f64)> {
        let rate: f64 = std::env::var("AWA_POINT_READS_PER_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|rate: &f64| *rate > 0.0)?;
        Some((
            Arc::new(Self {
                recent: Mutex::new(VecDeque::with_capacity(RECENT_CAPACITY)),
                older: Mutex::new(VecDeque::with_capacity(OLDER_CAPACITY)),
                observed: AtomicU64::new(0),
                found: AtomicU64::new(0),
                not_found: AtomicU64::new(0),
                errors: AtomicU64::new(0),
                latencies_ms: Mutex::new(VecDeque::new()),
            }),
            rate,
        ))
    }

    /// Called by the worker for every job it starts.
    pub fn observe(&self, job_id: i64) {
        let n = self.observed.fetch_add(1, Ordering::Relaxed);
        let mut recent = self.recent.lock().unwrap();
        if recent.len() == RECENT_CAPACITY {
            recent.pop_front();
        }
        recent.push_back(job_id);
        drop(recent);
        if n.is_multiple_of(OLDER_EVERY) {
            let mut older = self.older.lock().unwrap();
            if older.len() == OLDER_CAPACITY {
                older.pop_front();
            }
            older.push_back(job_id);
        }
    }

    fn pick(&self, draw: u64) -> Option<i64> {
        let pool = if draw.is_multiple_of(2) {
            &self.recent
        } else {
            &self.older
        };
        let ids = pool.lock().unwrap();
        if ids.is_empty() {
            return None;
        }
        // Older reads take the first half of the reservoir: ids old enough
        // that their slot may have been sealed or pruned.
        let span = if draw.is_multiple_of(2) {
            ids.len()
        } else {
            ids.len().div_ceil(2)
        };
        ids.get((draw / 2) as usize % span).copied()
    }

    fn record(&self, latency_ms: f64) {
        let mut window = self.latencies_ms.lock().unwrap();
        let now = Instant::now();
        window.push_back((now, latency_ms));
        while window
            .front()
            .is_some_and(|(at, _)| now.duration_since(*at) > Duration::from_secs(30))
        {
            window.pop_front();
        }
    }

    /// (p50, p99) over the last 30 s.
    pub fn percentiles(&self) -> Option<(f64, f64)> {
        let mut values: Vec<f64> = self
            .latencies_ms
            .lock()
            .unwrap()
            .iter()
            .map(|(_, ms)| *ms)
            .collect();
        if values.is_empty() {
            return None;
        }
        values.sort_by(f64::total_cmp);
        let at = |q: f64| {
            values[((q * (values.len() - 1) as f64).round() as usize).min(values.len() - 1)]
        };
        Some((at(0.50), at(0.99)))
    }

    /// Spawns `concurrency` readers that together issue `rate` reads/s.
    pub fn spawn(self: &Arc<Self>, pool: PgPool, rate: f64, concurrency: usize) {
        let per_reader = Duration::from_secs_f64(concurrency as f64 / rate);
        for reader in 0..concurrency {
            let this = Arc::clone(self);
            let pool = pool.clone();
            tokio::spawn(async move {
                let mut state = 0x9E37_79B9_7F4A_7C15_u64 ^ (reader as u64 + 1);
                let mut tick = tokio::time::interval(per_reader);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    let Some(job_id) = this.pick(state) else {
                        continue;
                    };
                    let started = Instant::now();
                    match awa_model::admin::get_job(&pool, job_id).await {
                        Ok(_) => {
                            this.found.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(awa_model::AwaError::JobNotFound { .. }) => {
                            this.not_found.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(err) => {
                            this.errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[awa] point read {job_id} failed: {err}");
                        }
                    }
                    this.record(started.elapsed().as_secs_f64() * 1000.0);
                }
            });
        }
    }
}
