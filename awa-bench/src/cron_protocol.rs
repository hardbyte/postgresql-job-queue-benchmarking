//! Control-plane cost probe. Requires a disposable database and #481-capable AWA.
//! Measures concurrent publication transactions, including pool wait and hashing.
use awa_model::{cron::PeriodicJob, cron_reconciliation as cron, migrations};
use sqlx::PgPool;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use uuid::Uuid;

async fn report(pool: &PgPool, id: Uuid, jobs: Option<&[PeriodicJob]>) -> f64 {
    let started = Instant::now();
    let mut tx = pool.begin().await.unwrap();
    cron::lock(&mut tx).await.unwrap();
    sqlx::query("INSERT INTO awa.runtime_instances(instance_id,pid,version,started_at,last_seen_at,snapshot_interval_ms,healthy,postgres_connected,poll_loop_alive,heartbeat_alive,maintenance_alive,shutting_down,leader,cron_protocol) VALUES ($1,1,'cron-bench',clock_timestamp(),clock_timestamp(),10000,true,true,true,true,true,false,false,1) ON CONFLICT(instance_id) DO UPDATE SET last_seen_at=clock_timestamp(),cron_protocol=EXCLUDED.cron_protocol")
        .bind(id).execute(&mut *tx).await.unwrap();
    if let Some(jobs) = jobs {
        let config = cron::PeriodicReconciliation::authoritative(
            "bench-owner",
            "bench-revision",
            Duration::ZERO,
        )
        .unwrap();
        cron::publish(&mut tx, id, &config, jobs).await.unwrap();
    }
    tx.commit().await.unwrap();
    started.elapsed().as_secs_f64() * 1000.0
}
async fn round(pool: &PgPool, ids: &[Uuid], jobs: Option<Arc<Vec<PeriodicJob>>>) -> Vec<f64> {
    let mut tasks = tokio::task::JoinSet::new();
    for id in ids {
        let (pool, jobs, id) = (pool.clone(), jobs.clone(), *id);
        tasks.spawn(async move { report(&pool, id, jobs.as_deref().map(Vec::as_slice)).await });
    }
    let mut values = vec![];
    while let Some(value) = tasks.join_next().await {
        values.push(value.unwrap());
    }
    values
}
fn stats(mut values: Vec<f64>) -> serde_json::Value {
    values.sort_by(f64::total_cmp);
    let percentile = |p: f64| values[((values.len() as f64 * p).ceil() as usize).saturating_sub(1)];
    serde_json::json!({"samples":values.len(),"p50_ms":percentile(0.5),"p99_ms":percentile(0.99),"max_ms":values.last()})
}
fn sizes(name: &str, fallback: &str) -> Vec<usize> {
    std::env::var(name)
        .unwrap_or(fallback.into())
        .split(',')
        .map(|v| v.parse().unwrap())
        .collect()
}
#[tokio::main]
async fn main() {
    assert_eq!(
        std::env::var("BENCH_DISPOSABLE_DATABASE").as_deref(),
        Ok("yes"),
        "Set BENCH_DISPOSABLE_DATABASE=yes: this probe replaces the awa schema"
    );
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(128)
        .acquire_timeout(Duration::from_secs(120))
        .connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    for fleet in sizes("CRON_FLEET_SIZES", "1,10,100") {
        for schedules in sizes("CRON_SCHEDULE_COUNTS", "10,1000,10000") {
            sqlx::raw_sql(
                "DROP SCHEMA IF EXISTS awa CASCADE; DROP SCHEMA IF EXISTS awa_qs CASCADE",
            )
            .execute(&pool)
            .await
            .unwrap();
            migrations::run(&pool).await.unwrap();
            let ids: Vec<_> = (0..fleet).map(|_| Uuid::new_v4()).collect();
            let jobs = Arc::new(
                (0..schedules)
                    .map(|i| {
                        PeriodicJob::builder(format!("schedule-{i:05}"), "0 0 * * *")
                            .build_raw("probe".into(), serde_json::json!({"example":i}))
                            .unwrap()
                    })
                    .collect::<Vec<_>>(),
            );
            let mut baseline = vec![];
            for _ in 0..3 {
                baseline.extend(round(&pool, &ids, None).await);
            }
            let initial = round(&pool, &ids, Some(jobs.clone())).await;
            let mut steady = vec![];
            for _ in 0..3 {
                steady.extend(round(&pool, &ids, Some(jobs.clone())).await);
            }
            let before = Instant::now();
            let plan = cron::reconcile(&pool, "bench-owner").await.unwrap();
            let reconcile_ms = before.elapsed().as_secs_f64() * 1000.;
            assert!(plan.applied && plan.retirements.is_empty());
            let retirement_publication = round(&pool, &ids, Some(Arc::new(vec![]))).await;
            let before = Instant::now();
            let retired = cron::reconcile(&pool, "bench-owner").await.unwrap();
            let retire_ms = before.elapsed().as_secs_f64() * 1000.;
            assert!(retired.applied && retired.retirements.len() == schedules);
            let bytes: i64 = sqlx::query_scalar("SELECT pg_total_relation_size('awa.cron_manifests') + pg_total_relation_size('awa.cron_declarations') + pg_total_relation_size('awa.cron_owners')").fetch_one(&pool).await.unwrap();
            println!(
                "{}",
                serde_json::json!({"fleet":fleet,"schedules":schedules,"snapshot_only":stats(baseline),"initial_publication":stats(initial),"steady_publication":stats(steady),"empty_publication":stats(retirement_publication),"reconcile_ms":reconcile_ms,"retire_ms":retire_ms,"protocol_relation_bytes":bytes,"schema":migrations::CURRENT_VERSION})
            );
        }
    }
    pool.close().await;
}
