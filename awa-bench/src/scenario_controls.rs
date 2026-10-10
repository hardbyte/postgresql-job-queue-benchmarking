//! Scenario controls (CONTRIBUTING_ADAPTERS.md "Scenario controls").
//!
//! Default-off: `JOB_FAILURE_CONTROL_FILE` tags jobs enqueued while a
//! failure plan is active as transient/poison failures;
//! `SCHEDULE_CONTROL_FILE` asks instance 0 to enqueue a scheduled herd.
//! Untagged jobs keep their payload and code path unchanged.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const HERD_BATCH_MAX: i64 = 500;

#[derive(Debug, Clone, Default, Deserialize)]
struct FailurePlan {
    #[serde(default)]
    transient_pct: f64,
    #[serde(default)]
    poison_pct: f64,
    #[serde(default)]
    transient_failures: Option<i16>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct HerdCommand {
    pub id: String,
    pub count: i64,
    pub run_at_ms: i64,
    #[serde(default)]
    pub spread_ms: i64,
}

/// Failure tag a producer attaches to a job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureTag {
    pub kind: &'static str,
    pub fail_attempts: Option<i16>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureOutcome {
    Retry,
    Exhausted,
}

pub fn failure_outcome(
    kind: &str,
    fail_attempts: Option<i16>,
    attempt: i16,
    max_attempts: i16,
) -> Option<FailureOutcome> {
    let failing = match kind {
        "poison" => true,
        "transient" => attempt <= fail_attempts.unwrap_or(1).max(1),
        _ => false,
    };
    if !failing {
        None
    } else if attempt >= max_attempts {
        Some(FailureOutcome::Exhausted)
    } else {
        Some(FailureOutcome::Retry)
    }
}

/// Mirrors adapter_common/bench_controls.py::herd_batches: (size, run_at_ms).
pub fn herd_batches(command: &HerdCommand) -> Vec<(usize, i64)> {
    let count = command.count.max(0);
    let batch = if command.spread_ms <= 0 {
        HERD_BATCH_MAX
    } else {
        (count * 100 / command.spread_ms).clamp(1, HERD_BATCH_MAX)
    };
    let mut out = Vec::new();
    let mut done = 0;
    while done < count {
        let size = batch.min(count - done);
        let offset = if command.spread_ms > 0 {
            command.spread_ms * done / count
        } else {
            0
        };
        out.push((size as usize, command.run_at_ms + offset));
        done += size;
    }
    out
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &str) -> Option<T> {
    let raw = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&raw).ok()
}

fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub struct ScenarioControls {
    failure_path: Option<String>,
    schedule_path: Option<String>,
    plan: Mutex<(Option<FailurePlan>, Option<Instant>)>,
    last_herd_id: Mutex<Option<String>>,

    failed_attempts: AtomicU64,
    retried_completions: AtomicU64,
    poison_exhausted: AtomicU64,
    scheduled_completions: AtomicU64,
    pub schedule_enqueued: AtomicU64,
    schedule_early: AtomicU64,
    /// -1 until a preload finishes.
    pub schedule_preload_ms: AtomicI64,
    lateness_ms: Mutex<Vec<f64>>,
    last_rates: Mutex<HashMap<&'static str, u64>>,
}

impl ScenarioControls {
    pub fn from_env() -> Self {
        let non_empty = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
        Self {
            failure_path: non_empty("JOB_FAILURE_CONTROL_FILE"),
            schedule_path: non_empty("SCHEDULE_CONTROL_FILE"),
            plan: Mutex::new((None, None)),
            last_herd_id: Mutex::new(None),
            failed_attempts: AtomicU64::new(0),
            retried_completions: AtomicU64::new(0),
            poison_exhausted: AtomicU64::new(0),
            scheduled_completions: AtomicU64::new(0),
            schedule_enqueued: AtomicU64::new(0),
            schedule_early: AtomicU64::new(0),
            schedule_preload_ms: AtomicI64::new(-1),
            lateness_ms: Mutex::new(Vec::new()),
            last_rates: Mutex::new(HashMap::new()),
        }
    }

    pub fn failure_enabled(&self) -> bool {
        self.failure_path.is_some()
    }

    pub fn schedule_enabled(&self) -> bool {
        self.schedule_path.is_some()
    }

    /// Failure tag for `seq` under the current plan (re-read at most 1/s).
    pub fn tag(&self, seq: i64) -> Option<FailureTag> {
        let path = self.failure_path.as_deref()?;
        let plan = {
            let mut guard = self.plan.lock().unwrap();
            let stale = guard
                .1
                .is_none_or(|read_at| read_at.elapsed() >= Duration::from_secs(1));
            if stale {
                *guard = (read_json::<FailurePlan>(path), Some(Instant::now()));
            }
            guard.0.clone()?
        };
        let poison_cut = (plan.poison_pct * 100.0).round() as i64;
        let transient_cut = poison_cut + (plan.transient_pct * 100.0).round() as i64;
        let bucket = seq.rem_euclid(10_000) * 7919 % 10_000;
        if bucket < poison_cut {
            Some(FailureTag {
                kind: "poison",
                fail_attempts: None,
            })
        } else if bucket < transient_cut {
            Some(FailureTag {
                kind: "transient",
                fail_attempts: Some(plan.transient_failures.unwrap_or(1).max(1)),
            })
        } else {
            None
        }
    }

    /// A herd command the first time its id is seen.
    pub fn poll_herd(&self) -> Option<HerdCommand> {
        let command: HerdCommand = read_json(self.schedule_path.as_deref()?)?;
        let mut last = self.last_herd_id.lock().unwrap();
        if last.as_deref() == Some(command.id.as_str()) {
            return None;
        }
        *last = Some(command.id.clone());
        Some(command)
    }

    pub fn on_start(&self, run_at_ms: Option<i64>) {
        let Some(run_at_ms) = run_at_ms else {
            return;
        };
        let lateness_ms = (now_epoch_ms() - run_at_ms) as f64;
        if lateness_ms < 0.0 {
            self.schedule_early.fetch_add(1, Ordering::Relaxed);
        }
        self.lateness_ms.lock().unwrap().push(lateness_ms.max(0.0));
    }

    pub fn record_failure(&self, kind: &str, outcome: FailureOutcome) {
        self.failed_attempts.fetch_add(1, Ordering::Relaxed);
        if outcome == FailureOutcome::Exhausted && kind == "poison" {
            self.poison_exhausted.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn on_complete(&self, kind: Option<&str>, run_at_ms: Option<i64>) {
        if kind == Some("transient") {
            self.retried_completions.fetch_add(1, Ordering::Relaxed);
        }
        if run_at_ms.is_some() {
            self.scheduled_completions.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn rate(&self, name: &'static str, value: u64, dt_s: f64) -> f64 {
        let mut last = self.last_rates.lock().unwrap();
        let previous = last.insert(name, value).unwrap_or(0);
        value.saturating_sub(previous) as f64 / dt_s
    }

    /// Extra sampler metrics as (name, value, window_s).
    pub fn metrics(&self, dt_s: f64, window_s: f64) -> Vec<(&'static str, f64, f64)> {
        let mut out = Vec::new();
        if self.failure_enabled() {
            out.push((
                "injected_failure_rate",
                self.rate("failed", self.failed_attempts.load(Ordering::Relaxed), dt_s),
                window_s,
            ));
            out.push((
                "retried_completion_rate",
                self.rate(
                    "retried",
                    self.retried_completions.load(Ordering::Relaxed),
                    dt_s,
                ),
                window_s,
            ));
            out.push((
                "poison_exhausted_rate",
                self.rate(
                    "exhausted",
                    self.poison_exhausted.load(Ordering::Relaxed),
                    dt_s,
                ),
                window_s,
            ));
        }
        if !self.schedule_enabled() {
            return out;
        }
        let values: Vec<f64> = {
            let mut guard = self.lateness_ms.lock().unwrap();
            guard.sort_by(f64::total_cmp);
            guard.clone()
        };
        let enqueued = self.schedule_enqueued.load(Ordering::Relaxed);
        if values.is_empty() && enqueued == 0 {
            return out;
        }
        out.push((
            "scheduled_completion_rate",
            self.rate(
                "scheduled",
                self.scheduled_completions.load(Ordering::Relaxed),
                dt_s,
            ),
            window_s,
        ));
        if let Some(&max_lateness) = values.last() {
            let n = values.len();
            let quantile = |p: f64| values[((p * (n - 1) as f64).round() as usize).min(n - 1)];
            out.push(("schedule_lateness_p50_ms", quantile(0.50), 0.0));
            out.push(("schedule_lateness_p95_ms", quantile(0.95), 0.0));
            out.push(("schedule_lateness_p99_ms", quantile(0.99), 0.0));
            out.push(("schedule_lateness_max_ms", max_lateness, 0.0));
        }
        out.push(("schedule_started_total", values.len() as f64, 0.0));
        out.push(("schedule_enqueued_total", enqueued as f64, 0.0));
        out.push((
            "schedule_early_total",
            self.schedule_early.load(Ordering::Relaxed) as f64,
            0.0,
        ));
        let preload_ms = self.schedule_preload_ms.load(Ordering::Relaxed);
        if preload_ms >= 0 {
            out.push(("schedule_preload_s", preload_ms as f64 / 1000.0, 0.0));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn herd_batches_cover_count_and_spread() {
        let burst = HerdCommand {
            id: "a".into(),
            count: 1201,
            run_at_ms: 1_000,
            spread_ms: 0,
        };
        let batches = herd_batches(&burst);
        assert_eq!(batches.iter().map(|(n, _)| n).sum::<usize>(), 1201);
        assert!(batches.iter().all(|&(_, at)| at == 1_000));

        let spread = HerdCommand {
            spread_ms: 10_000,
            ..burst
        };
        let batches = herd_batches(&spread);
        assert_eq!(batches.iter().map(|(n, _)| n).sum::<usize>(), 1201);
        assert_eq!(batches[0].1, 1_000);
        assert!(batches.last().unwrap().1 < 11_000);
    }

    #[test]
    fn outcome_matches_attempt_budget() {
        assert_eq!(
            failure_outcome("transient", Some(1), 1, 5),
            Some(FailureOutcome::Retry)
        );
        assert_eq!(failure_outcome("transient", Some(1), 2, 5), None);
        assert_eq!(
            failure_outcome("poison", None, 5, 5),
            Some(FailureOutcome::Exhausted)
        );
    }
}
