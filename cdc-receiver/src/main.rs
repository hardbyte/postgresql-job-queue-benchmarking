//! CDC bench receiver: terminates every system's sink delivery, timestamps
//! arrivals, verifies the per-key sequence ledger online, and emits JSONL
//! metric samples on stdout (the harness tailer stamps run/phase context).
//!
//! Endpoints:
//!   POST /sink/:consumer_id   — JSON array of canonical events
//!   POST /control             — per-consumer chaos: {consumer_id, mode, latency_ms}
//!   GET  /healthz
//!   GET  /ledger/:consumer_id — verifier state dump for the drain check
//!
//! Consumer profiles (design §3): heterogeneous by default. `CONSUMER_PROFILES`
//! like "1xfast,2xnormal,1xslow" — fast adds 0 ms handling time per request,
//! normal 25 ms, slow 250 ms. Chaos modes (dead / slow) layer on top via
//! /control and are what the consumer-dead / consumer-slow phase hooks call.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use hdrhistogram::Histogram;
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
struct Event {
    pk: i64,
    #[serde(default)]
    seq: Option<i64>,
    op: String,
    #[serde(default)]
    emitted_us: Option<i64>,
}

#[derive(Deserialize)]
struct ControlRequest {
    consumer_id: usize,
    mode: String, // "ok" | "dead" | "slow"
    #[serde(default)]
    latency_ms: Option<u64>,
}

struct ConsumerState {
    profile: String,
    profile_latency_ms: u64,
    dead: bool,
    chaos_latency_ms: Option<u64>,
    last_seq: HashMap<i64, i64>,
    deleted: HashSet<i64>,
    delivered: u64,
    delivered_bytes: u64,
    dups: u64,
    order_violations: u64,
    // Ring of per-tick histograms; merged on emit => rolling window.
    ring: Vec<Histogram<u64>>,
    ring_pos: usize,
    prev_delivered: u64,
    prev_delivered_bytes: u64,
}

struct App {
    consumers: Vec<Mutex<ConsumerState>>,
    sample_every_s: f64,
}

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn parse_profiles(spec: &str) -> Vec<(String, u64)> {
    // "1xfast,2xnormal,1xslow" -> [("fast",0),("normal",25),("normal",25),("slow",250)]
    let latency = |name: &str| match name {
        "fast" => 0u64,
        "normal" => 25,
        "slow" => 250,
        other => panic!("unknown consumer profile {other:?} (fast|normal|slow)"),
    };
    let mut out = Vec::new();
    for part in spec.split(',').filter(|p| !p.is_empty()) {
        let (count, name) = match part.split_once('x') {
            Some((n, name)) => (n.parse::<usize>().expect("profile count"), name),
            None => (1, part),
        };
        for _ in 0..count {
            out.push((name.to_string(), latency(name)));
        }
    }
    out
}

async fn sink(
    Path(cid): Path<usize>,
    State(app): State<Arc<App>>,
    body: Bytes,
) -> impl IntoResponse {
    let Some(slot) = app.consumers.get(cid) else {
        return (StatusCode::NOT_FOUND, "no such consumer").into_response();
    };
    let (dead, delay_ms) = {
        let c = slot.lock().unwrap();
        let delay = c.chaos_latency_ms.unwrap_or(c.profile_latency_ms);
        (c.dead, delay)
    };
    if dead {
        return (StatusCode::SERVICE_UNAVAILABLE, "consumer dead (chaos)").into_response();
    }
    let events: Vec<Event> = match serde_json::from_slice(&body) {
        Ok(events) => events,
        Err(err) => return (StatusCode::BAD_REQUEST, format!("bad body: {err}")).into_response(),
    };
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    let arrival_us = now_us();
    let mut c = slot.lock().unwrap();
    let ring_pos = c.ring_pos;
    for ev in &events {
        c.delivered += 1;
        if let Some(emitted) = ev.emitted_us {
            let e2e = (arrival_us - emitted).max(1) as u64;
            c.ring[ring_pos].record(e2e).ok();
        }
        match ev.op.as_str() {
            "delete" => {
                c.deleted.insert(ev.pk);
            }
            _ => {
                if let Some(seq) = ev.seq {
                    match c.last_seq.get(&ev.pk).copied() {
                        Some(last) if seq == last => c.dups += 1,
                        Some(last) if seq < last => {
                            c.dups += 1;
                            c.order_violations += 1;
                        }
                        _ => {
                            c.last_seq.insert(ev.pk, seq);
                        }
                    }
                }
            }
        }
    }
    c.delivered_bytes += body.len() as u64;
    (StatusCode::OK, "ok").into_response()
}

async fn control(
    State(app): State<Arc<App>>,
    Json(req): Json<ControlRequest>,
) -> impl IntoResponse {
    let Some(slot) = app.consumers.get(req.consumer_id) else {
        return (StatusCode::NOT_FOUND, "no such consumer");
    };
    let mut c = slot.lock().unwrap();
    match req.mode.as_str() {
        "ok" => {
            c.dead = false;
            c.chaos_latency_ms = None;
        }
        "dead" => c.dead = true,
        "slow" => {
            c.dead = false;
            c.chaos_latency_ms = Some(req.latency_ms.unwrap_or(250));
        }
        _ => return (StatusCode::BAD_REQUEST, "mode must be ok|dead|slow"),
    }
    eprintln!(
        "[receiver] control: consumer {} -> {} {:?}",
        req.consumer_id, req.mode, req.latency_ms
    );
    (StatusCode::OK, "ok")
}

async fn ledger(Path(cid): Path<usize>, State(app): State<Arc<App>>) -> impl IntoResponse {
    let Some(slot) = app.consumers.get(cid) else {
        return (StatusCode::NOT_FOUND, "no such consumer").into_response();
    };
    let c = slot.lock().unwrap();
    Json(json!({
        "profile": c.profile,
        "delivered": c.delivered,
        "dups": c.dups,
        "order_violations": c.order_violations,
        "last_seq": c.last_seq,
        "deleted": c.deleted,
    }))
    .into_response()
}

fn emit_samples(app: &App, dt: f64, window_s: f64) {
    let mut stdout_lines = Vec::new();
    for (cid, slot) in app.consumers.iter().enumerate() {
        let mut c = slot.lock().unwrap();
        let mut merged = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for h in &c.ring {
            merged.add(h).ok();
        }
        let mut push = |metric: &str, value: f64, w: f64| {
            stdout_lines.push(
                json!({
                    "kind": "consumer",
                    "subject_kind": "consumer",
                    "subject": format!("consumer:{cid}:{}", c.profile),
                    "instance_id": cid,
                    "metric": metric,
                    "value": value,
                    "window_s": w,
                })
                .to_string(),
            );
        };
        if !merged.is_empty() {
            push(
                "e2e_p50_ms",
                merged.value_at_quantile(0.50) as f64 / 1000.0,
                window_s,
            );
            push(
                "e2e_p95_ms",
                merged.value_at_quantile(0.95) as f64 / 1000.0,
                window_s,
            );
            push(
                "e2e_p99_ms",
                merged.value_at_quantile(0.99) as f64 / 1000.0,
                window_s,
            );
        }
        push(
            "delivery_rate",
            (c.delivered - c.prev_delivered) as f64 / dt,
            dt,
        );
        push(
            "delivery_bytes_rate",
            (c.delivered_bytes - c.prev_delivered_bytes) as f64 / dt,
            dt,
        );
        push("delivered_total", c.delivered as f64, 0.0);
        push("dup_events_total", c.dups as f64, 0.0);
        push("order_violations_total", c.order_violations as f64, 0.0);
        c.prev_delivered = c.delivered;
        c.prev_delivered_bytes = c.delivered_bytes;
        let next = (c.ring_pos + 1) % c.ring.len();
        c.ring[next].reset();
        c.ring_pos = next;
    }
    for line in stdout_lines {
        println!("{line}");
    }
}

async fn sampler(app: Arc<App>) {
    let every = app.sample_every_s;
    loop {
        // Clock-aligned ticks so cross-system samples line up on the plot
        // timebase (CONTRIBUTING_ADAPTERS.md cadence rule).
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs_f64();
        let next = (now / every).floor() * every + every;
        tokio::time::sleep(Duration::from_secs_f64(next - now)).await;
        let window_s = every * 6.0; // ring depth => rolling window
        emit_samples(&app, every, window_s);
    }
}

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("CDC_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(18080);
    let sample_every_s: f64 = std::env::var("SAMPLE_EVERY_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(5.0);
    let profiles_spec =
        std::env::var("CONSUMER_PROFILES").unwrap_or_else(|_| "1xnormal".to_string());
    let profiles = parse_profiles(&profiles_spec);

    let consumers = profiles
        .iter()
        .map(|(name, latency)| {
            Mutex::new(ConsumerState {
                profile: name.clone(),
                profile_latency_ms: *latency,
                dead: false,
                chaos_latency_ms: None,
                last_seq: HashMap::new(),
                deleted: HashSet::new(),
                delivered: 0,
                delivered_bytes: 0,
                dups: 0,
                order_violations: 0,
                ring: (0..6)
                    .map(|_| Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap())
                    .collect(),
                ring_pos: 0,
                prev_delivered: 0,
                prev_delivered_bytes: 0,
            })
        })
        .collect();
    let app = Arc::new(App {
        consumers,
        sample_every_s,
    });

    eprintln!(
        "[receiver] up on :{port} with {} consumers ({profiles_spec})",
        app.consumers.len()
    );

    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/sink/:cid", post(sink))
        .route("/control", post(control))
        .route("/ledger/:cid", get(ledger))
        .with_state(app.clone());

    tokio::spawn(sampler(app.clone()));

    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .unwrap();
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
            tokio::select! {
                _ = sigterm.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
        })
        .await
        .unwrap();
    // Final flush so the last partial window isn't lost.
    emit_samples(&app, app.sample_every_s, app.sample_every_s * 6.0);
}
