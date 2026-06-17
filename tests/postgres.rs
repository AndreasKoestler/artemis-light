//! Integration tests for the PostgreSQL-backed [`Store`], gated behind the
//! `postgres` feature so the default `cargo test` needs neither Docker nor a
//! running PostgreSQL (postgres-store.TESTING.1/.2). Each test provisions a
//! throwaway PostgreSQL container via testcontainers.
#![cfg(feature = "postgres")]

use std::sync::Arc;

use alloy::primitives::U256;
use alloy::sol;
use anyhow::Result;
use artemis_light::persistence::{
    PersistExt, PersistableCollector, PostgresStore, Record, Row, SqlType, SqlValue, SqliteStore,
    Store, TableSchema,
};
use artemis_light::types::{Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

sol! {
    // A one-field event used to drive `Persisted` deterministically, mirroring
    // the SQLite persistence tests.
    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    event ValueSet(uint256 indexed value);
}

fn value_event(value: u64) -> ValueSet {
    ValueSet {
        value: U256::from(value),
    }
}

/// A scripted [`PersistableCollector`] that yields a fixed live stream at a
/// fixed tip — enough to drive `Persisted` (replay-then-live) without Anvil.
#[derive(Default)]
struct FakeCollector {
    live: Vec<(u64, u64)>,
    tip: u64,
}

impl FakeCollector {
    fn live(mut self, events: Vec<(u64, u64)>) -> Self {
        self.live = events;
        self
    }
    fn tip(mut self, tip: u64) -> Self {
        self.tip = tip;
        self
    }
}

#[async_trait]
impl PersistableCollector<ValueSet> for FakeCollector {
    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (u64, ValueSet)>> {
        let events: Vec<_> = self
            .live
            .iter()
            .map(|&(b, v)| (b, value_event(v)))
            .collect();
        Ok(Box::pin(futures::stream::iter(events)))
    }

    async fn query_range(
        &self,
        from: u64,
        to: u64,
    ) -> Result<CollectorStream<'_, (u64, ValueSet)>> {
        if from > to {
            anyhow::bail!("inverted range: from {from} > to {to}");
        }
        // These tests leave no RPC gap, so no backfill events are produced.
        Ok(Box::pin(futures::stream::iter(Vec::new())))
    }

    async fn tip(&self) -> Result<u64> {
        Ok(self.tip)
    }
}

/// Persist one `ValueSet` event at `block` through a `Record`, as a prior run
/// would have, so a later subscribe replays it.
async fn seed(store: &Arc<PostgresStore>, block: u64, value: u64) {
    let record = Record::<ValueSet>::new(None).unwrap();
    let row = record.encode(&value_event(value)).unwrap();
    let schema = record.schema().unwrap();
    store.write_block(&schema, block, vec![row]).await.unwrap();
}

/// One-column text schema mirroring the SQLite store tests' `value_set` table.
fn value_set_schema() -> TableSchema {
    TableSchema::new("value_set").col("value", SqlType::Text)
}

/// Start a throwaway PostgreSQL container and return it with a connection URL.
/// The returned [`ContainerAsync`] guard MUST be kept alive for the duration of
/// the test — dropping it stops (and removes) the container.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let container = Postgres::default()
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("map postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Happy path: a written block can be read back via `replay`, in ascending
/// order (postgres-store.PGSTORE.2/.3/.4).
#[tokio::test]
async fn postgres_store_write_then_replay_round_trips() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    store
        .write_block(
            &schema,
            7,
            vec![
                Row(vec![SqlValue::Text("0x2a".into())]),
                Row(vec![SqlValue::Text("0x2b".into())]),
            ],
        )
        .await
        .unwrap();

    let rows = store.replay(&schema, 100).await.unwrap();
    assert_eq!(
        rows,
        vec![
            Row(vec![SqlValue::Text("0x2a".into())]),
            Row(vec![SqlValue::Text("0x2b".into())]),
        ]
    );
}

/// `last_block` reports the highest written block, and `None` before any write
/// (postgres-store.PGSTORE.5).
#[tokio::test]
async fn postgres_store_last_block_returns_written_height() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    // Nothing stored yet: the progress table does not exist (SQLSTATE 42P01).
    assert_eq!(store.last_block(&schema.table).await.unwrap(), None);

    store
        .write_block(&schema, 5, vec![Row(vec![SqlValue::Text("a".into())])])
        .await
        .unwrap();
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(5));

    store
        .write_block(&schema, 9, vec![Row(vec![SqlValue::Text("b".into())])])
        .await
        .unwrap();
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(9));
}

/// Connecting to an unreachable server returns an error rather than a
/// half-open store (postgres-store.PGSTORE.1-1).
#[tokio::test]
async fn postgres_connect_invalid_url_errors() {
    // Port 1 has nothing listening; the eager pool connection is refused.
    let result = PostgresStore::connect("postgres://postgres:postgres@127.0.0.1:1/postgres").await;
    assert!(
        result.is_err(),
        "connect to an unreachable server must error"
    );
}

/// A row whose value count does not match the column count is rejected and the
/// whole block rolls back, leaving prior committed data and the watermark
/// untouched (postgres-store.PGSTORE.6, ATOMICITY.1, DURABILITY.2;
/// RowShapeMatchesColumnCount → ShapeMismatchRejected, rollback → StoreWriteFailed).
#[tokio::test]
async fn write_block_shape_mismatch_rolls_back() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema(); // one column

    // Block 5 is written cleanly.
    store
        .write_block(&schema, 5, vec![Row(vec![SqlValue::Text("ok".into())])])
        .await
        .unwrap();

    // Block 9's second row has too few values for the schema, so the shape
    // guard bails partway through the batch.
    let result = store
        .write_block(
            &schema,
            9,
            vec![
                Row(vec![SqlValue::Text("good".into())]),
                Row(vec![]), // missing the `value` column
            ],
        )
        .await;
    assert!(result.is_err(), "malformed batch must fail");

    // Block 9 rolled back entirely: only block 5's row survives and the
    // watermark still points at block 5 (gap-free prefix preserved).
    assert_eq!(
        store.replay(&schema, 100).await.unwrap(),
        vec![Row(vec![SqlValue::Text("ok".into())])]
    );
    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(5));
}

/// `replay` on a table that has never been written returns an empty vec, not an
/// error — the undefined-table SQLSTATE (42P01) is classified as "nothing
/// stored" (postgres-store.PGSTORE.4-1; ReadEmptyOrMissingTable).
#[tokio::test]
async fn replay_missing_table_returns_empty() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    let rows = store.replay(&schema, 100).await.unwrap();
    assert!(
        rows.is_empty(),
        "replay of a never-written table must be empty"
    );
}

/// `last_block` on a table that has never been written returns `None` — the
/// progress table does not yet exist (42P01) (postgres-store.PGSTORE.5;
/// ReadEmptyOrMissingTable).
#[tokio::test]
async fn last_block_missing_table_returns_none() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();

    assert_eq!(store.last_block("never_written").await.unwrap(), None);
}

/// A block number at the top of the supported range (`i64::MAX`) round-trips
/// through the BIGINT column without loss (postgres-store.TYPES.2; the
/// supported range is [0, i64::MAX]).
#[tokio::test]
async fn block_number_near_i64_max_round_trips() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();
    let height = i64::MAX as u64; // top of the supported block-height range

    store
        .write_block(
            &schema,
            height,
            vec![Row(vec![SqlValue::Text("edge".into())])],
        )
        .await
        .unwrap();

    assert_eq!(store.last_block(&schema.table).await.unwrap(), Some(height));
    assert_eq!(
        store.replay(&schema, height).await.unwrap(),
        vec![Row(vec![SqlValue::Text("edge".into())])]
    );
}

/// An `Arc<PostgresStore>` drives the `Persisted` collector wrapper unchanged
/// (via the existing blanket `impl Store for Arc<T>`): on subscribe, stored
/// PostgreSQL history is replayed first, then the live tip follows
/// (postgres-store.PGSTORE.8).
#[tokio::test]
async fn persisted_drives_arc_postgres_store() {
    let (_container, url) = start_postgres().await;
    let store = Arc::new(PostgresStore::connect(&url).await.unwrap());

    // Two events stored by a "previous run".
    seed(&store, 5, 1).await;
    seed(&store, 6, 2).await;

    // Tip is the last stored block (no RPC gap); the live stream carries block 7.
    let collector = FakeCollector::default().live(vec![(7, 3)]).tip(6);
    let persisted = collector.with_persistence(store.clone());

    let events: Vec<ValueSet> = persisted.subscribe().await.unwrap().collect().await;
    assert_eq!(
        events,
        vec![value_event(1), value_event(2), value_event(3)],
        "replay of PostgreSQL history then the live tip, via the unchanged Persisted wrapper"
    );
}

/// Events persisted to PostgreSQL survive a "restart": a fresh `PostgresStore`
/// opened on the same database replays the prior events and reports the
/// unchanged watermark (postgres-store.DURABILITY.1).
#[tokio::test]
async fn postgres_restart_replays_prior_events() {
    let (_container, url) = start_postgres().await;
    let schema = value_set_schema();

    // First "process": write two blocks, then drop the store (simulated exit).
    {
        let store = PostgresStore::connect(&url).await.unwrap();
        store
            .write_block(&schema, 5, vec![Row(vec![SqlValue::Text("a".into())])])
            .await
            .unwrap();
        store
            .write_block(&schema, 9, vec![Row(vec![SqlValue::Text("b".into())])])
            .await
            .unwrap();
    }

    // Second "process": a new store on the same database sees the prior state.
    let restarted = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(restarted.last_block(&schema.table).await.unwrap(), Some(9));
    assert_eq!(
        restarted.replay(&schema, 100).await.unwrap(),
        vec![
            Row(vec![SqlValue::Text("a".into())]),
            Row(vec![SqlValue::Text("b".into())]),
        ]
    );
}

/// The same event stream persisted to PostgreSQL and to SQLite replays to
/// logically identical `Row`/`SqlValue` sequences in identical order, including
/// a `Numeric` column that decodes to `SqlValue::Text` on both backends
/// (postgres-store.PARITY.1, TYPES.1).
#[tokio::test]
async fn sqlite_postgres_replay_parity() {
    let (_container, url) = start_postgres().await;
    let pg = PostgresStore::connect(&url).await.unwrap();
    let sqlite = SqliteStore::connect("sqlite::memory:").await.unwrap();

    // Multi-column schema covering the dialect-sensitive cases: a Numeric column
    // (TEXT in PG; NUMERIC affinity in SQLite — both decode back to
    // SqlValue::Text), a plain Text column, and an Integer column.
    let schema = TableSchema::new("evt")
        .col("amount", SqlType::Numeric)
        .col("note", SqlType::Text)
        .col("count", SqlType::Integer);

    // A hex-string amount: not a well-formed decimal/real literal, so SQLite's
    // NUMERIC affinity leaves it as TEXT, matching PG's TEXT column.
    let block1 = vec![Row(vec![
        SqlValue::Text("0x2a".into()),
        SqlValue::Text("first".into()),
        SqlValue::Integer(10),
    ])];
    let block2 = vec![Row(vec![
        SqlValue::Text("0x2b".into()),
        SqlValue::Text("second".into()),
        SqlValue::Integer(20),
    ])];

    for store_writes in [&pg as &dyn Store, &sqlite as &dyn Store] {
        store_writes
            .write_block(&schema, 1, block1.clone())
            .await
            .unwrap();
        store_writes
            .write_block(&schema, 2, block2.clone())
            .await
            .unwrap();
    }

    let pg_rows = pg.replay(&schema, 100).await.unwrap();
    let sqlite_rows = sqlite.replay(&schema, 100).await.unwrap();

    assert_eq!(
        pg_rows, sqlite_rows,
        "PostgreSQL and SQLite must replay identical rows in identical order"
    );
    // And both match the originally written rows.
    let expected: Vec<Row> = block1.into_iter().chain(block2).collect();
    assert_eq!(pg_rows, expected);
}
