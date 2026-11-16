//! awa-only diagnostics for focused A/B runs.
//!
//! `MaintenanceCapture` keeps awa's own OpenTelemetry metrics in memory so the
//! sampler can report cumulative ring maintenance outcomes (`prune` and
//! `rotate` attempts by ring and outcome), each ring's generation, and the
//! lanes pinning the queue ring (`awa.maintenance.prune.pinned_ready_age`,
//! reported as the oldest age and the number of pinning lanes). Only this
//! process's runtime is covered: with several replicas, the values live on
//! whichever replica leads maintenance.
//!
//! `PointReads` drives `admin::get_job` at `AWA_POINT_READS_PER_S` against
//! jobs this process has started: half recent ids, half older ids, so reads
//! land both on live slots and on slots that rotation has sealed.
//!
//! `AWA_SOAK_DIAGNOSTICS=1` adds finalize, rescue and pool diagnostics for
//! release soaks (see [`soak_metrics`]), a count of awa's `Failed to complete
//! job` and `pool timed out` log events, and a pool-occupancy sampler.

use opentelemetry_sdk::metrics::data::{AggregatedMetrics, HistogramDataPoint, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};
use sqlx::PgPool;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAINTENANCE_METRICS: [&str; 2] = [
    "awa.maintenance.prune.attempts",
    "awa.maintenance.rotate.attempts",
];

pub struct MaintenanceCapture {
    exporter: InMemoryMetricExporter,
    /// Histogram (count, bucket counts) at the previous sample, so soak
    /// percentiles cover one sample window rather than the whole run.
    previous_histograms: Mutex<HashMap<&'static str, (u64, Vec<u64>)>>,
    previous_maintenance_sum: Mutex<f64>,
    previous_applied: Mutex<Option<(f64, Instant)>>,
}

static CAPTURE: OnceLock<MaintenanceCapture> = OnceLock::new();
static PROVIDER: OnceLock<SdkMeterProvider> = OnceLock::new();

/// The process's meter provider, flushed before each soak sample so awa's
/// counters line up with the adapter's own (`finalize_pending`).
pub fn register_provider(provider: &SdkMeterProvider) {
    let _ = PROVIDER.set(provider.clone());
}

/// A reader to add to the process's meter provider; its exports are read
/// back by [`maintenance_totals`].
pub fn maintenance_reader(interval: Duration) -> PeriodicReader<InMemoryMetricExporter> {
    let exporter = InMemoryMetricExporter::default();
    let reader = PeriodicReader::builder(exporter.clone())
        .with_interval(interval)
        .build();
    let _ = CAPTURE.set(MaintenanceCapture {
        exporter,
        previous_histograms: Mutex::new(HashMap::new()),
        previous_maintenance_sum: Mutex::new(0.0),
        previous_applied: Mutex::new(None),
    });
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

pub fn soak_diagnostics_enabled() -> bool {
    matches!(
        std::env::var("AWA_SOAK_DIAGNOSTICS").as_deref(),
        Ok("1" | "true" | "yes" | "on")
    )
}

/// Latest cumulative maintenance counters as
/// `maint_<prune|rotate>_<ring>_<outcome>_total`, or empty before the
/// first export. With `soak`, also the [`soak_metrics`].
pub fn maintenance_totals(soak: bool) -> Vec<(String, f64)> {
    let Some(capture) = CAPTURE.get() else {
        return Vec::new();
    };
    if soak {
        if let Some(provider) = PROVIDER.get() {
            let _ = tokio::task::block_in_place(|| provider.force_flush());
        }
    }
    let Ok(exports) = capture.exporter.get_finished_metrics() else {
        return Vec::new();
    };
    // Cumulative temporality: the last export holds the running totals.
    let mut totals: BTreeMap<String, f64> = BTreeMap::new();
    if let Some(latest) = exports.last() {
        for scope in latest.scope_metrics() {
            for metric in scope.metrics() {
                let name = metric.name();
                if soak {
                    soak_metrics(capture, name, metric.data(), &mut totals);
                }
                match (name, metric.data()) {
                    ("awa.ring.generation", AggregatedMetrics::I64(MetricData::Gauge(gauge))) => {
                        for point in gauge.data_points() {
                            let ring = attribute(point.attributes(), "awa.ring")
                                .unwrap_or_else(|| "unknown".into());
                            totals.insert(format!("ring_generation_{ring}"), point.value() as f64);
                        }
                        continue;
                    }
                    (
                        "awa.maintenance.prune.pinned_ready_age",
                        AggregatedMetrics::F64(MetricData::Gauge(gauge)),
                    ) => {
                        let ages: Vec<f64> = gauge.data_points().map(|p| p.value()).collect();
                        let pinned = ages.iter().filter(|age| **age > 0.0).count();
                        let oldest = ages.iter().copied().fold(0.0_f64, f64::max);
                        totals.insert("prune_pinned_ready_age_max_s".into(), oldest);
                        totals.insert("prune_pinned_lanes".into(), pinned as f64);
                        continue;
                    }
                    _ => {}
                }
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
    // Without a fresh export there is nothing to compare leadership against.
    if soak && !exports.is_empty() {
        let maintenance_sum: f64 = totals
            .iter()
            .filter(|(key, _)| key.starts_with("maint_"))
            .map(|(_, value)| *value)
            .sum();
        let mut previous = capture.previous_maintenance_sum.lock().unwrap();
        totals.insert(
            "maintenance_active".into(),
            f64::from(u8::from(maintenance_sum > *previous)),
        );
        *previous = maintenance_sum;
        let handler_returns = HANDLER_RETURNS.load(Ordering::Relaxed) as f64;
        let failed = FINALIZE_FAILED.load(Ordering::Relaxed) as f64;
        totals.insert("handler_returns_total".into(), handler_returns);
        totals.insert("finalize_failed_total".into(), failed);
        totals.insert(
            "finalize_failed_pool_timeout_total".into(),
            FINALIZE_FAILED_POOL_TIMEOUT.load(Ordering::Relaxed) as f64,
        );
        totals.insert(
            "pool_timeout_log_total".into(),
            POOL_TIMEOUT_LOGS.load(Ordering::Relaxed) as f64,
        );
        let applied: f64 = [
            "awa_jobs_completed_total",
            "awa_jobs_retried_total",
            "awa_jobs_failed_total",
            "awa_jobs_waiting_external_total",
        ]
        .iter()
        .filter_map(|key| totals.get(*key))
        .sum();
        let pending = (handler_returns - applied - failed).max(0.0);
        totals.insert("finalize_pending".into(), pending);
        // Little's law over the sample window: finalizes waiting / finalizes
        // applied per second is the mean time a finalize waits.
        let now = Instant::now();
        let mut previous = capture.previous_applied.lock().unwrap();
        if let Some((previous_applied, at)) = *previous {
            let rate = (applied - previous_applied) / now.duration_since(at).as_secs_f64();
            if rate > 0.0 {
                totals.insert("finalize_latency_est_ms".into(), pending / rate * 1000.0);
            }
        }
        *previous = Some((applied, now));
    }
    // Keep only the newest export so memory stays flat over long runs.
    capture.exporter.reset();
    totals.into_iter().collect()
}

/// Cumulative counters summed over their attributes, as `(otel name, key)`.
const SOAK_COUNTERS: [(&str, &str); 8] = [
    ("awa.job.completed", "awa_jobs_completed_total"),
    ("awa.job.retried", "awa_jobs_retried_total"),
    ("awa.job.failed", "awa_jobs_failed_total"),
    (
        "awa.job.waiting_external",
        "awa_jobs_waiting_external_total",
    ),
    (
        "awa.completion.deadlock_retry",
        "awa_finalize_deadlock_retries_total",
    ),
    ("awa.completion.flushes", "awa_completion_flushes_total"),
    ("awa.heartbeat.batches", "awa_heartbeat_batches_total"),
    (
        "awa.maintenance.prune.claim_cursor_advances",
        "awa_claim_cursor_advances_total",
    ),
];

/// Latency histograms reported as `<key>_p99_ms` over the last sample
/// window (the upper bound of the bucket holding the 99th percentile) and
/// `<key>_max_ms` since the process started.
const SOAK_HISTOGRAMS: [(&str, &str); 2] = [
    ("awa.completion.flush_duration", "completion_flush"),
    ("awa.dispatch.claim_duration", "claim_query"),
];

/// Release-soak diagnostics from awa's own metrics, bounded to a handful of
/// keys per process: job outcome totals as awa applied them, completion
/// deadlock retries, batched completion flushes, heartbeat batches, rescues
/// and the oldest rescued attempt's age by rescue kind, jobs in flight, and
/// windowed p99 / run max for completion flushes and claim queries.
/// awa exports nothing for direct (unbatched) finalizes, the finalize gate,
/// heartbeat age or pool waits; `finalize_pending`, the log counters, the
/// pool sampler and the harness's heartbeat-age query stand in for them.
fn soak_metrics(
    capture: &MaintenanceCapture,
    name: &str,
    data: &AggregatedMetrics,
    totals: &mut BTreeMap<String, f64>,
) {
    if let Some((_, key)) = SOAK_COUNTERS.iter().find(|(otel, _)| *otel == name) {
        let total = match data {
            AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                sum.data_points().map(|p| p.value() as f64).sum()
            }
            _ => return,
        };
        totals.insert((*key).into(), total);
        return;
    }
    if let Some((otel, key)) = SOAK_HISTOGRAMS.iter().find(|(otel, _)| *otel == name) {
        if let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = data {
            let points: Vec<&HistogramDataPoint<f64>> = histogram.data_points().collect();
            let Some(first) = points.first() else {
                return;
            };
            let bounds: Vec<f64> = first.bounds().collect();
            let mut buckets = vec![0_u64; bounds.len() + 1];
            let mut count = 0;
            let mut max = 0.0_f64;
            for point in &points {
                count += point.count();
                for (slot, value) in buckets.iter_mut().zip(point.bucket_counts()) {
                    *slot += value;
                }
                max = max.max(point.max().unwrap_or(0.0));
            }
            let mut previous = capture.previous_histograms.lock().unwrap();
            let (previous_count, previous_buckets) = previous
                .get(otel)
                .cloned()
                .unwrap_or((0, vec![0; buckets.len()]));
            let window_count = count.saturating_sub(previous_count);
            if window_count > 0 {
                let target = (window_count as f64 * 0.99).ceil() as u64;
                let mut seen = 0;
                for (index, bucket) in buckets.iter().enumerate() {
                    seen +=
                        bucket.saturating_sub(previous_buckets.get(index).copied().unwrap_or(0));
                    if seen >= target {
                        let upper = bounds.get(index).copied().unwrap_or(max);
                        totals.insert(format!("{key}_p99_ms"), upper * 1000.0);
                        break;
                    }
                }
            }
            totals.insert(format!("{key}_max_ms"), max * 1000.0);
            previous.insert(otel, (count, buckets));
        }
        return;
    }
    match (name, data) {
        ("awa.maintenance.rescues", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
            for point in sum.data_points() {
                let kind = attribute(point.attributes(), "awa.rescue.kind")
                    .unwrap_or_else(|| "unknown".into());
                *totals
                    .entry(format!("awa_rescues_{kind}_total"))
                    .or_default() += point.value() as f64;
            }
        }
        (
            "awa.maintenance.rescue.oldest_attempt_age",
            AggregatedMetrics::F64(MetricData::Gauge(gauge)),
        ) => {
            for point in gauge.data_points() {
                let kind = attribute(point.attributes(), "awa.rescue.kind")
                    .unwrap_or_else(|| "unknown".into());
                totals.insert(format!("rescue_oldest_attempt_age_{kind}_s"), point.value());
            }
        }
        ("awa.job.in_flight", AggregatedMetrics::I64(MetricData::Sum(sum))) => {
            totals.insert(
                "awa_jobs_in_flight".into(),
                sum.data_points().map(|p| p.value() as f64).sum(),
            );
        }
        _ => {}
    }
}

/// Every return from a bench handler, counted against awa's applied
/// outcomes for `finalize_pending`.
pub static HANDLER_RETURNS: AtomicU64 = AtomicU64::new(0);
static FINALIZE_FAILED: AtomicU64 = AtomicU64::new(0);
static FINALIZE_FAILED_POOL_TIMEOUT: AtomicU64 = AtomicU64::new(0);
static POOL_TIMEOUT_LOGS: AtomicU64 = AtomicU64::new(0);

/// Counts awa's WARN/ERROR events that matter for finalize health:
/// `Failed to complete job` (a finalize that gave up; the attempt is left to
/// rescue), those whose error was a pool acquire timeout, and any event
/// mentioning `pool timed out`. It sees events whatever `RUST_LOG` prints.
pub struct LogCounter;

#[derive(Default)]
struct EventText {
    message: String,
    fields: String,
}

impl tracing::field::Visit for EventText {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}");
        } else {
            self.fields.push_str(&format!(" {value:?}"));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_owned();
        } else {
            self.fields.push(' ');
            self.fields.push_str(value);
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for LogCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut text = EventText::default();
        event.record(&mut text);
        let pool_timeout =
            text.fields.contains("pool timed out") || text.message.contains("pool timed out");
        if pool_timeout {
            POOL_TIMEOUT_LOGS.fetch_add(1, Ordering::Relaxed);
        }
        if text.message.contains("Failed to complete job") {
            FINALIZE_FAILED.fetch_add(1, Ordering::Relaxed);
            if pool_timeout {
                FINALIZE_FAILED_POOL_TIMEOUT.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Samples the pool every 100 ms: the most connections in use and the share
/// of samples with every connection in use, per sample window.
pub struct PoolSampler {
    max_in_use: AtomicU32,
    samples: AtomicU64,
    saturated: AtomicU64,
}

impl PoolSampler {
    pub fn spawn(pool: PgPool, max_connections: u32) -> Arc<Self> {
        let sampler = Arc::new(Self {
            max_in_use: AtomicU32::new(0),
            samples: AtomicU64::new(0),
            saturated: AtomicU64::new(0),
        });
        let this = Arc::clone(&sampler);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            loop {
                tick.tick().await;
                let in_use = pool.size().saturating_sub(pool.num_idle() as u32);
                this.max_in_use.fetch_max(in_use, Ordering::Relaxed);
                this.samples.fetch_add(1, Ordering::Relaxed);
                if in_use >= max_connections {
                    this.saturated.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        sampler
    }

    /// Window metrics; resets the window.
    pub fn take(&self) -> Vec<(String, f64)> {
        let samples = self.samples.swap(0, Ordering::Relaxed);
        let saturated = self.saturated.swap(0, Ordering::Relaxed);
        let max_in_use = self.max_in_use.swap(0, Ordering::Relaxed);
        if samples == 0 {
            return Vec::new();
        }
        vec![
            ("pool_in_use_max".into(), f64::from(max_in_use)),
            (
                "pool_saturated_frac".into(),
                saturated as f64 / samples as f64,
            ),
        ]
    }
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

/// Haystack jobs completed after an operator lane op moved them onto a bench
/// queue, and those completed twice.
pub static HAYSTACK_COMPLETED: AtomicU64 = AtomicU64::new(0);

pub fn haystack_completions() -> &'static DuplicateTracker {
    static TRACKER: OnceLock<DuplicateTracker> = OnceLock::new();
    TRACKER.get_or_init(|| DuplicateTracker::new(0, i64::from(u32::MAX)))
}

/// In-process duplicate execution evidence, keyed by job id (or, for
/// haystack jobs, sequence number) in `base..base + span`: a job starting
/// while an earlier attempt of it is still running here (a live claim was
/// rescued and re-dispatched), and a job completing a second time. A job
/// whose attempts land on different replicas or process lifetimes is not
/// seen.
pub struct DuplicateTracker {
    base: i64,
    span: i64,
    running: Mutex<HashSet<i64>>,
    completed: Mutex<Vec<u64>>,
    pub concurrent_starts: AtomicU64,
    pub repeat_completions: AtomicU64,
}

impl DuplicateTracker {
    /// Tracks completions of keys in `base..base + span`.
    pub fn new(base: i64, span: i64) -> Self {
        Self {
            base,
            span,
            running: Mutex::new(HashSet::new()),
            completed: Mutex::new(Vec::new()),
            concurrent_starts: AtomicU64::new(0),
            repeat_completions: AtomicU64::new(0),
        }
    }

    pub fn start(&self, key: i64) {
        if !self.running.lock().unwrap().insert(key) {
            self.concurrent_starts.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn finish(&self, key: i64, completed: bool) {
        self.running.lock().unwrap().remove(&key);
        if !completed || key < self.base || key - self.base >= self.span {
            return;
        }
        let index = (key - self.base) as usize;
        let (word, bit) = (index / 64, 1_u64 << (index % 64));
        let mut bits = self.completed.lock().unwrap();
        if word >= bits.len() {
            bits.resize(word + 1 + word / 2, 0);
        }
        if bits[word] & bit != 0 {
            self.repeat_completions.fetch_add(1, Ordering::Relaxed);
        }
        bits[word] |= bit;
    }

    pub fn metrics(&self) -> Vec<(String, f64)> {
        vec![
            (
                "duplicate_concurrent_starts_total".into(),
                self.concurrent_starts.load(Ordering::Relaxed) as f64,
            ),
            (
                "duplicate_completions_total".into(),
                self.repeat_completions.load(Ordering::Relaxed) as f64,
            ),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_tracker_counts_overlap_and_repeat_completion() {
        let tracker = DuplicateTracker::new(1_000, 1_000);
        tracker.start(1_005);
        tracker.start(1_005);
        tracker.finish(1_005, true);
        tracker.finish(1_005, true);
        tracker.start(1_200);
        tracker.finish(1_200, false);
        tracker.start(1_200);
        tracker.finish(1_200, true);
        tracker.start(5_000);
        tracker.finish(5_000, true);
        tracker.start(5_000);
        tracker.finish(5_000, true);
        assert_eq!(tracker.concurrent_starts.load(Ordering::Relaxed), 1);
        assert_eq!(tracker.repeat_completions.load(Ordering::Relaxed), 1);
    }
}
