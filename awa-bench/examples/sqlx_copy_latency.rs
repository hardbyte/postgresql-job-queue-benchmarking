//! Reproduce COPY latency independently of AWA or the benchmark pacer.
use sqlx::Connection;
use std::time::Instant;
#[tokio::main]
async fn main() {
    let mut conn = sqlx::PgConnection::connect(&std::env::var("DATABASE_URL").unwrap())
        .await
        .unwrap();
    sqlx::query("CREATE TEMP TABLE copy_latency(value TEXT)")
        .execute(&mut conn)
        .await
        .unwrap();
    let payload = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n".repeat(20).into_bytes();
    let mut select_ms = vec![];
    let mut copy_ms = vec![];
    for _ in 0..20 {
        let started = Instant::now();
        sqlx::query("SELECT 1").execute(&mut conn).await.unwrap();
        select_ms.push(started.elapsed().as_secs_f64() * 1000.0);
        let started = Instant::now();
        let mut copy = conn
            .copy_in_raw("COPY copy_latency(value) FROM STDIN")
            .await
            .unwrap();
        copy.send(payload.clone()).await.unwrap();
        copy.finish().await.unwrap();
        copy_ms.push(started.elapsed().as_secs_f64() * 1000.0);
    }
    select_ms.sort_by(f64::total_cmp);
    copy_ms.sort_by(f64::total_cmp);
    println!(
        "{}",
        serde_json::json!({"select_median_ms":select_ms[10],"copy_median_ms":copy_ms[10],"copy_samples_ms":copy_ms})
    );
    assert!(
        copy_ms[10] < 20.0,
        "COPY takes a delayed-ACK-sized pause on a local database"
    );
}
