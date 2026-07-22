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
    table: Option<String>,
    #[serde(default)]
    tx_id: Option<i64>,
    #[serde(default)]
    balance: Option<i64>,
    #[serde(default)]
    emitted_us: Option<i64>,
}

#[derive(Default)]
struct KeyState {
    seq: i64,
    balance: Option<i64>,
    deleted: bool,
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
    // Table-name interning: verifier keys are (table_idx, pk).
    table_ids: HashMap<String, u16>,
    keys: HashMap<(u16, i64), KeyState>,
    // tx_id -> distinct (table, pk) keys delivered for that tx. A tx is
    // complete once all `tx_events` of its rows have been seen — counting
    // distinct keys (not fresh events) tolerates at-least-once reordering,
    // where a row can arrive as a stale dup yet must still count toward its
    // transaction. A genuinely partial tx leaves keys missing at drain.
    open_txs: HashMap<i64, HashSet<(u16, i64)>>,
    completed_tx_ids: HashSet<i64>,
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

fn track_tx_group(
    open_tx_groups: &mut HashMap<i64, HashSet<(u16, i64)>>,
    completed_tx_ids: &mut HashSet<i64>,
    tx_id: i64,
    key: (u16, i64),
    expected_rows: usize,
) {
    if completed_tx_ids.contains(&tx_id) {
        return;
    }
    let seen = open_tx_groups.entry(tx_id).or_default();
    seen.insert(key);
    if seen.len() >= expected_rows {
        open_tx_groups.remove(&tx_id);
        completed_tx_ids.insert(tx_id);
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Envelope {
    /// JSON array of canonical events (in-repo adapters emit this directly).
    Canonical,
    /// One Debezium change event per POST (before/after/source/op), with or
    /// without the schema wrapper; `null` bodies are tombstones to ack.
    Debezium,
    /// Sequin webhook sink: single object or {"data": [...]} batch of
    /// {record, changes, action, metadata}.
    Sequin,
}

struct App {
    consumers: Vec<Mutex<ConsumerState>>,
    sample_every_s: f64,
    envelope: Envelope,
    // Expected replicated events per source tx (0 = tx tracking off).
    tx_events: u32,
}

fn field_i64(row: &serde_json::Value, name: &str) -> Option<i64> {
    row.get(name).and_then(|v| v.as_i64())
}

fn decode_debezium_one(value: &serde_json::Value) -> Result<Option<Event>, String> {
    if value.is_null() {
        return Ok(None); // tombstone — ack with no event
    }
    // JsonConverter with schemas enabled wraps as {"schema":…, "payload":…}.
    let payload = value
        .get("payload")
        .filter(|_| value.get("schema").is_some())
        .unwrap_or(value);
    if payload.is_null() {
        return Ok(None);
    }
    let op = payload
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or("debezium event missing op")?;
    let (op_name, row) = match op {
        "c" | "r" => ("insert", payload.get("after")),
        "u" => ("update", payload.get("after")),
        "d" => ("delete", payload.get("before")),
        other => return Err(format!("unknown debezium op {other:?}")),
    };
    let row = row
        .filter(|r| !r.is_null())
        .ok_or("debezium event missing row image")?;
    let source = payload.get("source");
    let table = source.map(|s| {
        format!(
            "{}.{}",
            s.get("schema").and_then(|v| v.as_str()).unwrap_or(""),
            s.get("table").and_then(|v| v.as_str()).unwrap_or(""),
        )
    });
    Ok(Some(Event {
        pk: field_i64(row, "pk").ok_or("debezium row missing pk")?,
        seq: field_i64(row, "seq"),
        op: op_name.to_string(),
        table,
        tx_id: field_i64(row, "tx_id"),
        balance: field_i64(row, "balance"),
        emitted_us: field_i64(row, "emitted_us"),
    }))
}

fn decode_debezium(body: &[u8]) -> Result<Vec<Event>, String> {
    if body.is_empty() {
        return Ok(vec![]); // tombstone via http sink: empty body
    }
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("debezium body: {e}"))?;
    // Batch mode (debezium.sink.http.batch.enabled) posts a JSON array;
    // elements may themselves be JSON-encoded strings of the value.
    let items: Vec<serde_json::Value> = match value {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| match item {
                serde_json::Value::String(inner) => {
                    serde_json::from_str(&inner).map_err(|e| format!("debezium batch item: {e}"))
                }
                other => Ok(other),
            })
            .collect::<Result<_, String>>()?,
        single => vec![single],
    };
    let mut events = Vec::with_capacity(items.len());
    for item in &items {
        if let Some(event) = decode_debezium_one(item)? {
            events.push(event);
        }
    }
    Ok(events)
}

fn decode_sequin(body: &[u8]) -> Result<Vec<Event>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("sequin body: {e}"))?;
    let items: Vec<&serde_json::Value> = match value.get("data").and_then(|d| d.as_array()) {
        Some(batch) => batch.iter().collect(),
        None => vec![&value],
    };
    let mut events = Vec::with_capacity(items.len());
    for item in items {
        let action = item
            .get("action")
            .and_then(|v| v.as_str())
            .ok_or("sequin event missing action")?;
        let op = match action {
            "insert" | "read" => "insert",
            "update" => "update",
            "delete" => "delete",
            other => return Err(format!("unknown sequin action {other:?}")),
        };
        let row = item.get("record").ok_or("sequin event missing record")?;
        let table = item.get("metadata").map(|m| {
            format!(
                "{}.{}",
                m.get("table_schema").and_then(|v| v.as_str()).unwrap_or(""),
                m.get("table_name").and_then(|v| v.as_str()).unwrap_or(""),
            )
        });
        events.push(Event {
            pk: field_i64(row, "pk").ok_or("sequin record missing pk")?,
            seq: field_i64(row, "seq"),
            op: op.to_string(),
            table,
            tx_id: field_i64(row, "tx_id"),
            balance: field_i64(row, "balance"),
            emitted_us: field_i64(row, "emitted_us"),
        });
    }
    Ok(events)
}

fn decode_events(envelope: Envelope, body: &[u8]) -> Result<Vec<Event>, String> {
    match envelope {
        Envelope::Canonical => {
            serde_json::from_slice(body).map_err(|e| format!("canonical body: {e}"))
        }
        Envelope::Debezium => decode_debezium(body),
        Envelope::Sequin => decode_sequin(body),
    }
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
    let events: Vec<Event> = match decode_events(app.envelope, &body) {
        Ok(events) => events,
        Err(err) => {
            eprintln!("[receiver] consumer {cid}: undecodable event: {err}");
            return (StatusCode::BAD_REQUEST, err).into_response();
        }
    };
    // Timestamp at receipt, before the simulated handling delay: e2e_* measures
    // pipeline lag, not the injected stimulus. The delayed 200 below still
    // exerts backpressure on the sender.
    let arrival_us = now_us();
    if delay_ms > 0 {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
    }
    let tx_events = app.tx_events;
    let mut c = slot.lock().unwrap();
    let ring_pos = c.ring_pos;
    for ev in &events {
        c.delivered += 1;
        if let Some(emitted) = ev.emitted_us {
            let e2e = (arrival_us - emitted).max(1) as u64;
            c.ring[ring_pos].record(e2e).ok();
        }
        let table = ev.table.as_deref().unwrap_or("");
        let tid = match c.table_ids.get(table) {
            Some(tid) => *tid,
            None => {
                let tid = c.table_ids.len() as u16;
                c.table_ids.insert(table.to_string(), tid);
                tid
            }
        };
        let key = (tid, ev.pk);
        match ev.op.as_str() {
            "delete" => {
                c.keys.entry(key).or_default().deleted = true;
            }
            _ => {
                if let Some(seq) = ev.seq {
                    let mut dup = false;
                    let mut violation = false;
                    let state = c.keys.entry(key).or_default();
                    if state.deleted {
                        // A pk is never reused by any workload (events/ledger/
                        // outbox all allocate monotonically), so an upsert
                        // arriving after a delete is necessarily a reordered
                        // redelivery — count it as an out-of-order dup but keep
                        // the tombstone. Without this, an at-least-once buffer
                        // that replays a key's delete before its earlier
                        // upserts (Sequin on sink recovery) would resurrect the
                        // row and the drain check would report phantom loss.
                        dup = true;
                        violation = true;
                    } else if seq <= state.seq {
                        dup = true;
                        violation = seq < state.seq;
                    } else {
                        state.seq = seq;
                        if ev.balance.is_some() {
                            state.balance = ev.balance;
                        }
                    }
                    c.dups += dup as u64;
                    c.order_violations += violation as u64;
                }
            }
        }
        // Transaction grouping (ledger mode): a tx is complete once all of
        // its distinct rows have been delivered. Counting distinct keys —
        // rather than fresh events — tolerates at-least-once reordering (a
        // row replayed out of order still counts toward its tx) while a
        // genuinely partial delivery leaves the set short at drain. This
        // checks eventual grouping by application tx_id, not atomic visibility.
        // tx_id 0 marks preload/snapshot rows — not a live transaction.
        if tx_events > 0 {
            if let Some(tx_id) = ev.tx_id.filter(|id| *id > 0) {
                let state = &mut *c;
                track_tx_group(
                    &mut state.open_txs,
                    &mut state.completed_tx_ids,
                    tx_id,
                    key,
                    tx_events as usize,
                );
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
    // Invert the interning: table -> pk -> {seq, balance, deleted}.
    let mut tables: HashMap<&str, HashMap<i64, serde_json::Value>> = HashMap::new();
    let names: HashMap<u16, &str> = c
        .table_ids
        .iter()
        .map(|(name, tid)| (*tid, name.as_str()))
        .collect();
    for ((tid, pk), state) in &c.keys {
        tables.entry(names[tid]).or_default().insert(
            *pk,
            json!({"seq": state.seq, "balance": state.balance, "deleted": state.deleted}),
        );
    }
    let completed_tx_id_min = c.completed_tx_ids.iter().min();
    let completed_tx_id_max = c.completed_tx_ids.iter().max();
    Json(json!({
        "profile": c.profile,
        "delivered": c.delivered,
        "dups": c.dups,
        "order_violations": c.order_violations,
        "open_txs": c.open_txs.len(),
        "complete_tx_groups": c.completed_tx_ids.len(),
        "completed_tx_id_min": completed_tx_id_min,
        "completed_tx_id_max": completed_tx_id_max,
        "tables": tables,
    }))
    .into_response()
}

fn emit_samples(app: &App, dt: f64, window_s: f64) {
    let tx_tracking = app.tx_events > 0;
    let mut stdout_lines = Vec::new();
    for (cid, slot) in app.consumers.iter().enumerate() {
        let mut c = slot.lock().unwrap();
        // 2h ceiling: heal-phase replay delivers events whose e2e age is the
        // whole outage duration; a 60s bound silently dropped them and made
        // every system's heal p99 saturate at the same ~66s bucket ceiling.
        let mut merged = Histogram::<u64>::new_with_bounds(1, 7_200_000_000, 3).unwrap();
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
        if tx_tracking {
            push("incomplete_tx_groups_current", c.open_txs.len() as f64, 0.0);
            push(
                "complete_tx_groups_total",
                c.completed_tx_ids.len() as f64,
                0.0,
            );
        }
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
    let envelope = match std::env::var("ENVELOPE").as_deref() {
        Ok("debezium") => Envelope::Debezium,
        Ok("sequin") => Envelope::Sequin,
        Ok("canonical") | Err(_) => Envelope::Canonical,
        Ok(other) => panic!("unknown ENVELOPE {other:?}"),
    };

    let consumers = profiles
        .iter()
        .map(|(name, latency)| {
            Mutex::new(ConsumerState {
                profile: name.clone(),
                profile_latency_ms: *latency,
                dead: false,
                chaos_latency_ms: None,
                table_ids: HashMap::new(),
                keys: HashMap::new(),
                open_txs: HashMap::new(),
                completed_tx_ids: HashSet::new(),
                delivered: 0,
                delivered_bytes: 0,
                dups: 0,
                order_violations: 0,
                ring: (0..6)
                    .map(|_| Histogram::<u64>::new_with_bounds(1, 7_200_000_000, 3).unwrap())
                    .collect(),
                ring_pos: 0,
                prev_delivered: 0,
                prev_delivered_bytes: 0,
            })
        })
        .collect();
    let tx_events: u32 = std::env::var("TX_EVENTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let app = Arc::new(App {
        consumers,
        sample_every_s,
        envelope,
        tx_events,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_transaction_group_is_not_reopened_or_double_counted() {
        let mut open = HashMap::new();
        let mut completed = HashSet::new();
        for key in [(0, 10), (1, 20), (1, 30)] {
            track_tx_group(&mut open, &mut completed, 7, key, 3);
        }
        assert!(open.is_empty());
        assert_eq!(completed, HashSet::from([7]));

        for key in [(0, 10), (1, 20), (1, 30)] {
            track_tx_group(&mut open, &mut completed, 7, key, 3);
        }
        assert!(open.is_empty());
        assert_eq!(completed, HashSet::from([7]));
    }

    #[test]
    fn duplicate_key_does_not_complete_transaction_group() {
        let mut open = HashMap::new();
        let mut completed = HashSet::new();
        for _ in 0..3 {
            track_tx_group(&mut open, &mut completed, 7, (0, 10), 3);
        }
        assert_eq!(open.get(&7).map(HashSet::len), Some(1));
        assert!(completed.is_empty());
    }
}
