//! Supabase ETL CDC bench adapter.
//!
//! The "roll your own consumer per slot" arm: one `etl` Pipeline per
//! consumer (slot-per-consumer), each with a custom Destination that
//! converts streamed events to the canonical envelope and POSTs them to
//! the harness receiver. Retries forever on sink errors (5xx = chaos), so
//! a dead consumer stalls the pipeline — and its slot — exactly like the
//! raw baseline.
//!
//! Env: DATABASE_URL, SINK_URL, CONSUMER_COUNT, PUBLICATION (default cdc_pub).
//! Stdout: one descriptor line once all pipelines are started.

use std::time::Duration;

use etl::config::{BatchConfig, PgConnectionConfig, PipelineConfig, TlsConfig};
use etl::data::{Cell, OldTableRow, TableRow, UpdatedTableRow};
use etl::destination::{
    Destination, DestinationTableMetadata, DestinationTableSchemaStatus, DestinationWriteStatus,
    DropTableForCopyResult, WriteEventsDurability, WriteEventsResult, WriteTableRowsResult,
};
use etl::error::EtlResult;
use etl::event::Event;
use etl::pipeline::Pipeline;
use etl::schema::ReplicatedTableSchema;
use etl::store::{MemoryStore, SharedStateStore};

#[derive(Clone)]
struct HttpDestination<S> {
    client: reqwest::Client,
    sink_url: String,
    store: S,
}

fn cell_i64(cell: &Cell) -> Option<i64> {
    match cell {
        Cell::I64(v) => Some(*v),
        Cell::I32(v) => Some(*v as i64),
        Cell::I16(v) => Some(*v as i64),
        _ => None,
    }
}

fn row_to_canonical(op: &str, schema: &ReplicatedTableSchema, row: &TableRow) -> serde_json::Value {
    let mut pk = None;
    let mut seq = None;
    let mut tx_id = None;
    let mut emitted_us = None;
    // Positional cells zipped with the replicated column order. A key-only
    // delete image carries just the leading replica-identity columns; `pk`
    // is the table's first column so the zip stays aligned for it.
    for (column, cell) in schema.column_schemas().zip(row.values().iter()) {
        match column.name.as_str() {
            "pk" => pk = cell_i64(cell),
            "seq" => seq = cell_i64(cell),
            "tx_id" => tx_id = cell_i64(cell),
            "emitted_us" => emitted_us = cell_i64(cell),
            _ => {}
        }
    }
    serde_json::json!({
        "table": schema.name().to_string(),
        "op": op,
        "pk": pk,
        "seq": seq,
        "tx_id": tx_id,
        "emitted_us": emitted_us,
    })
}

impl<S> HttpDestination<S>
where
    S: SharedStateStore,
{
    async fn post_with_retry(&self, events: Vec<serde_json::Value>) {
        if events.is_empty() {
            return;
        }
        let body = serde_json::to_vec(&events).expect("serialize canonical batch");
        let mut backoff = Duration::from_millis(100);
        loop {
            let sent = self
                .client
                .post(&self.sink_url)
                .header("content-type", "application/json")
                .body(body.clone())
                .send()
                .await;
            if let Ok(resp) = sent {
                if resp.status().is_success() {
                    return;
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(1));
        }
    }

    /// Mirror of MemoryDestination::sync_destination_table_metadata — ETL
    /// tracks per-table destination metadata through the state store; real
    /// destinations persist it, we just acknowledge schema application.
    async fn sync_metadata(&self, schema: &ReplicatedTableSchema) -> EtlResult<()> {
        let table_id = schema.id();
        let existing = self.store.get_destination_table_metadata(table_id).await?;
        let metadata = match existing {
            Some(metadata)
                if metadata.snapshot_id == schema.inner().snapshot_id
                    && metadata.replication_mask == *schema.replication_mask() =>
            {
                return Ok(());
            }
            Some(metadata) => metadata
                .with_schema_change(
                    schema.inner().snapshot_id,
                    schema.replication_mask().clone(),
                    DestinationTableSchemaStatus::Applied,
                )
                .to_applied(),
            None => DestinationTableMetadata::new_applied(
                format!("cdc_bench_http_{}", table_id.into_inner()),
                schema.inner().snapshot_id,
                schema.replication_mask().clone(),
            ),
        };
        self.store
            .store_destination_table_metadata(table_id, metadata)
            .await?;
        Ok(())
    }
}

impl<S> Destination for HttpDestination<S>
where
    S: SharedStateStore + Send + Sync,
{
    fn name() -> &'static str {
        "cdc-bench-http"
    }

    async fn drop_table_for_copy(
        &self,
        _schema: &ReplicatedTableSchema,
        async_result: DropTableForCopyResult<()>,
    ) -> EtlResult<()> {
        async_result.send(Ok(()));
        Ok(())
    }

    async fn write_table_rows(
        &self,
        schema: &ReplicatedTableSchema,
        table_rows: Vec<TableRow>,
        async_result: WriteTableRowsResult,
    ) -> EtlResult<()> {
        self.sync_metadata(schema).await?;
        // Initial copy rows would be snapshot data; the harness recreates
        // the source table empty before pipeline start, but ship any rows
        // as inserts anyway so a future snapshot scenario measures honestly.
        let canonical: Vec<_> = table_rows
            .iter()
            .map(|row| row_to_canonical("insert", schema, row))
            .collect();
        self.post_with_retry(canonical).await;
        async_result.send(Ok(DestinationWriteStatus::Durable));
        Ok(())
    }

    async fn write_events(
        &self,
        events: Vec<Event>,
        _durability: WriteEventsDurability,
        async_result: WriteEventsResult,
    ) -> EtlResult<()> {
        let mut canonical = Vec::with_capacity(events.len());
        for event in &events {
            match event {
                Event::Insert(e) => {
                    self.sync_metadata(&e.replicated_table_schema).await?;
                    canonical.push(row_to_canonical(
                        "insert",
                        &e.replicated_table_schema,
                        &e.table_row,
                    ));
                }
                Event::Update(e) => {
                    self.sync_metadata(&e.replicated_table_schema).await?;
                    match &e.updated_table_row {
                        UpdatedTableRow::Full(row) => canonical.push(row_to_canonical(
                            "update",
                            &e.replicated_table_schema,
                            row,
                        )),
                        UpdatedTableRow::Partial(_) => {
                            eprintln!("[etl-cdc-bench] WARN dropping partial update image");
                        }
                    }
                }
                Event::Delete(e) => {
                    self.sync_metadata(&e.replicated_table_schema).await?;
                    if let Some(OldTableRow::Full(row) | OldTableRow::Key(row)) = &e.old_table_row {
                        canonical.push(row_to_canonical("delete", &e.replicated_table_schema, row));
                    }
                }
                Event::Relation(e) => {
                    self.sync_metadata(&e.replicated_table_schema).await?;
                }
                _ => {}
            }
        }
        self.post_with_retry(canonical).await;
        async_result.send(Ok(DestinationWriteStatus::Durable));
        Ok(())
    }
}

fn parse_database_url(url: &str) -> PgConnectionConfig {
    // postgres://user:pass@host:port/dbname
    let rest = url.split("://").nth(1).expect("DATABASE_URL scheme");
    let (creds, hostpart) = rest.split_once('@').expect("DATABASE_URL credentials");
    let (user, password) = creds.split_once(':').unwrap_or((creds, ""));
    let (hostport, dbname) = hostpart.split_once('/').expect("DATABASE_URL dbname");
    let (host, port) = hostport.split_once(':').unwrap_or((hostport, "5432"));
    PgConnectionConfig {
        host: host.to_string(),
        port: port.parse().expect("port"),
        name: dbname.to_string(),
        username: user.to_string(),
        password: Some(password.to_string().into()),
        tls: TlsConfig {
            enabled: false,
            trusted_root_certs: String::new(),
        },
        keepalive: Default::default(),
        hostaddr: None,
    }
}

#[tokio::main]
async fn main() {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let sink_base = std::env::var("SINK_URL").expect("SINK_URL");
    let consumer_count: usize = std::env::var("CONSUMER_COUNT")
        .unwrap_or_else(|_| "2".into())
        .parse()
        .expect("CONSUMER_COUNT");
    let publication = std::env::var("PUBLICATION").unwrap_or_else(|_| "cdc_pub".into());

    let mut handles = Vec::new();
    for cid in 0..consumer_count {
        let store = MemoryStore::new();
        let destination = HttpDestination {
            client: reqwest::Client::new(),
            sink_url: format!("{}/sink/{}", sink_base.trim_end_matches('/'), cid),
            store: store.clone(),
        };
        let config = PipelineConfig {
            id: (cid + 1) as u64,
            publication_name: publication.clone(),
            pg_connection: parse_database_url(&database_url),
            store_pg_connection: None,
            batch: BatchConfig {
                max_fill_ms: 20,
                ..Default::default()
            },
            table_error_retry_delay_ms: 1_000,
            table_error_retry_max_attempts: 10,
            max_table_sync_workers: 2,
            max_copy_connections_per_table: PipelineConfig::DEFAULT_MAX_COPY_CONNECTIONS_PER_TABLE,
            memory_refresh_interval_ms: PipelineConfig::DEFAULT_MEMORY_REFRESH_INTERVAL_MS,
            replication_lag_refresh_interval_ms:
                PipelineConfig::DEFAULT_REPLICATION_LAG_REFRESH_INTERVAL_MS,
            memory_backpressure: Some(Default::default()),
            table_sync_copy: Default::default(),
            invalidated_slot_behavior: Default::default(),
            run_source_migrations: true,
        };
        let mut pipeline = Pipeline::new(config, store, destination);
        // Start sequentially: concurrent starts race on the etl source
        // migrations (CREATE SCHEMA etl collides under parallel creation).
        pipeline.start().await.expect("pipeline start");
        eprintln!("[etl-cdc-bench] pipeline {cid} started");
        handles.push(tokio::spawn(async move {
            pipeline.wait().await.expect("pipeline wait");
        }));
    }

    // SIGTERM → exit promptly; slots persist, which is fine for the bench.
    tokio::spawn(async {
        let mut sigterm =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        sigterm.recv().await;
        eprintln!("[etl-cdc-bench] SIGTERM, exiting");
        std::process::exit(0);
    });

    println!(
        "{}",
        serde_json::json!({
            "kind": "descriptor",
            "system": "supabase-etl",
            "pipelines": consumer_count,
            "publication": publication,
        })
    );

    for handle in handles {
        handle.await.expect("pipeline task");
    }
}
