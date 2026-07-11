//! Integration tests for the PostgreSQL-backed [`Store`], gated behind the
//! `postgres` feature so the default `cargo test` needs neither Docker nor a
//! running PostgreSQL. Each test provisions a throwaway PostgreSQL container via
//! testcontainers.
#![cfg(feature = "postgres")]

use std::sync::Arc;
use std::time::Duration;

use alloy::primitives::U256;
use alloy::sol;
use anyhow::Result;
use artemis_light::persistence::{
    BlockPosition, PersistExt, PersistableCollector, PostgresStore, Record, Row, SqlType, SqlValue,
    SqliteStore, Store, TableSchema,
};
use artemis_light::types::{Collector, CollectorStream};
use async_trait::async_trait;
use futures::StreamExt;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
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
    type Pos = BlockPosition;

    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (BlockPosition, ValueSet)>> {
        let events: Vec<_> = self
            .live
            .iter()
            .map(|&(b, v)| (BlockPosition(b), value_event(v)))
            .collect();
        Ok(Box::pin(futures::stream::iter(events)))
    }

    async fn query_range(
        &self,
        from: BlockPosition,
        to: BlockPosition,
    ) -> Result<CollectorStream<'_, (BlockPosition, ValueSet)>> {
        if from.0 > to.0 {
            anyhow::bail!("inverted range: from {} > to {}", from.0, to.0);
        }
        // These tests leave no RPC gap, so no backfill events are produced.
        Ok(Box::pin(futures::stream::iter(Vec::new())))
    }

    async fn tip(&self) -> Result<BlockPosition> {
        Ok(BlockPosition(self.tip))
    }
}

/// Persist one `ValueSet` event at `block` through a `Record`, as a prior run
/// would have, so a later subscribe replays it.
async fn seed(store: &Arc<PostgresStore>, block: u64, value: u64) {
    let record = Record::<ValueSet>::new(None).unwrap();
    let row = record.encode(&value_event(value)).unwrap();
    let schema = record.schema().unwrap();
    store
        .write(&schema, BlockPosition(block), vec![row])
        .await
        .unwrap();
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
/// order.
#[tokio::test]
async fn postgres_store_write_then_replay_round_trips() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    store
        .write(
            &schema,
            BlockPosition(7),
            vec![
                Row(vec![SqlValue::Text("0x2a".into())]),
                Row(vec![SqlValue::Text("0x2b".into())]),
            ],
        )
        .await
        .unwrap();

    let rows = store.replay(&schema, BlockPosition(100)).await.unwrap();
    assert_eq!(
        rows,
        vec![
            Row(vec![SqlValue::Text("0x2a".into())]),
            Row(vec![SqlValue::Text("0x2b".into())]),
        ]
    );
}

/// `stored_position` reports the highest written block, and `None` before any
/// write.
#[tokio::test]
async fn postgres_store_last_block_returns_written_height() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    // Nothing stored yet: the progress table does not exist (SQLSTATE 42P01).
    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        None::<BlockPosition>
    );

    store
        .write(
            &schema,
            BlockPosition(5),
            vec![Row(vec![SqlValue::Text("a".into())])],
        )
        .await
        .unwrap();
    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(5))
    );

    store
        .write(
            &schema,
            BlockPosition(9),
            vec![Row(vec![SqlValue::Text("b".into())])],
        )
        .await
        .unwrap();
    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(9))
    );
}

/// Connecting to an unreachable server returns an error rather than a
/// half-open store.
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
/// untouched.
#[tokio::test]
async fn write_block_shape_mismatch_rolls_back() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema(); // one column

    // Block 5 is written cleanly.
    store
        .write(
            &schema,
            BlockPosition(5),
            vec![Row(vec![SqlValue::Text("ok".into())])],
        )
        .await
        .unwrap();

    // Block 9's second row has too few values for the schema, so the shape
    // guard bails partway through the batch.
    let result = store
        .write(
            &schema,
            BlockPosition(9),
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
        store.replay(&schema, BlockPosition(100)).await.unwrap(),
        vec![Row(vec![SqlValue::Text("ok".into())])]
    );
    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(5))
    );
}

/// `replay` on a table that has never been written returns an empty vec, not an
/// error — the undefined-table SQLSTATE (42P01) is classified as "nothing
/// stored".
#[tokio::test]
async fn replay_missing_table_returns_empty() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    let rows = store.replay(&schema, BlockPosition(100)).await.unwrap();
    assert!(
        rows.is_empty(),
        "replay of a never-written table must be empty"
    );
}

/// `stored_position` on a table that has never been written returns `None` — the
/// progress table does not yet exist (42P01).
#[tokio::test]
async fn last_block_missing_table_returns_none() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();

    assert_eq!(
        store.stored_position("never_written").await.unwrap(),
        None::<BlockPosition>
    );
}

/// A block number at the top of the supported range (`i64::MAX`) round-trips
/// through the BIGINT column without loss — the supported range is
/// [0, i64::MAX].
#[tokio::test]
async fn block_number_near_i64_max_round_trips() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();
    let height = i64::MAX as u64; // top of the supported block-height range

    store
        .write(
            &schema,
            BlockPosition(height),
            vec![Row(vec![SqlValue::Text("edge".into())])],
        )
        .await
        .unwrap();

    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(height))
    );
    assert_eq!(
        store.replay(&schema, BlockPosition(height)).await.unwrap(),
        vec![Row(vec![SqlValue::Text("edge".into())])]
    );
}

/// An `Arc<PostgresStore>` drives the `Persisted` collector wrapper unchanged
/// (via the existing blanket `impl Store for Arc<T>`): on subscribe, stored
/// PostgreSQL history is replayed first, then the live tip follows.
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
/// unchanged watermark.
#[tokio::test]
async fn postgres_restart_replays_prior_events() {
    let (_container, url) = start_postgres().await;
    let schema = value_set_schema();

    // First "process": write two blocks, then drop the store (simulated exit).
    {
        let store = PostgresStore::connect(&url).await.unwrap();
        store
            .write(
                &schema,
                BlockPosition(5),
                vec![Row(vec![SqlValue::Text("a".into())])],
            )
            .await
            .unwrap();
        store
            .write(
                &schema,
                BlockPosition(9),
                vec![Row(vec![SqlValue::Text("b".into())])],
            )
            .await
            .unwrap();
    }

    // Second "process": a new store on the same database sees the prior state.
    let restarted = PostgresStore::connect(&url).await.unwrap();
    assert_eq!(
        restarted.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(9))
    );
    assert_eq!(
        restarted.replay(&schema, BlockPosition(100)).await.unwrap(),
        vec![
            Row(vec![SqlValue::Text("a".into())]),
            Row(vec![SqlValue::Text("b".into())]),
        ]
    );
}

/// The same event stream persisted to PostgreSQL and to SQLite replays to
/// logically identical `Row`/`SqlValue` sequences in identical order, including
/// a `Numeric` column that decodes to `SqlValue::Text` on both backends.
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
            .write(&schema, BlockPosition(1), block1.clone())
            .await
            .unwrap();
        store_writes
            .write(&schema, BlockPosition(2), block2.clone())
            .await
            .unwrap();
    }

    let pg_rows = pg.replay(&schema, BlockPosition(100)).await.unwrap();
    let sqlite_rows = sqlite.replay(&schema, BlockPosition(100)).await.unwrap();

    assert_eq!(
        pg_rows, sqlite_rows,
        "PostgreSQL and SQLite must replay identical rows in identical order"
    );
    // And both match the originally written rows.
    let expected: Vec<Row> = block1.into_iter().chain(block2).collect();
    assert_eq!(pg_rows, expected);
}

/// A store built from a caller-supplied pool via `with_pool` behaves identically
/// to one built from a URL via `connect`: identical blocks written through each
/// yield identical replay rows and identical `stored_position`. The injected pool
/// is multi-connection (`max_connections(5)`) to exercise the no-cap contract —
/// the constructor neither inspects nor overrides the count — and the empty-state
/// reads before any write confirm the missing-table classification is unchanged
/// on the injected path.
#[tokio::test]
async fn with_pool_store_matches_connect_store_parity() {
    // One container backs both stores; two distinct event tables keep their
    // watermarks independent (the shared `_artemis_progress` keys by table name),
    // so the stores do not interfere.
    let (_container, url) = start_postgres().await;

    // URL path: the existing single-connection `connect` constructor.
    let url_store = PostgresStore::connect(&url).await.unwrap();
    let url_schema = TableSchema::new("events_url").col("value", SqlType::Text);

    // Injected path: a caller-owned multi-connection pool handed to `with_pool`.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    let pool_store = PostgresStore::with_pool(pool);
    let pool_schema = TableSchema::new("events_pool").col("value", SqlType::Text);

    // Empty state before any write: `stored_position` is `None` and `replay` is empty
    // on both paths (missing-table classification via SQLSTATE 42P01, unchanged).
    assert_eq!(
        url_store.stored_position(&url_schema.table).await.unwrap(),
        None::<BlockPosition>
    );
    assert_eq!(
        pool_store
            .stored_position(&pool_schema.table)
            .await
            .unwrap(),
        None::<BlockPosition>
    );
    assert!(
        url_store
            .replay(&url_schema, BlockPosition(100))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        pool_store
            .replay(&pool_schema, BlockPosition(100))
            .await
            .unwrap()
            .is_empty()
    );

    // Identical blocks written through each store into its own event table.
    let block1 = vec![
        Row(vec![SqlValue::Text("0x2a".into())]),
        Row(vec![SqlValue::Text("0x2b".into())]),
    ];
    let block2 = vec![Row(vec![SqlValue::Text("0x2c".into())])];

    for (store, schema) in [(&url_store, &url_schema), (&pool_store, &pool_schema)] {
        store
            .write(schema, BlockPosition(5), block1.clone())
            .await
            .unwrap();
        store
            .write(schema, BlockPosition(9), block2.clone())
            .await
            .unwrap();
    }

    // Parity through the public `Store` API only: equal replay row-vectors and
    // equal `stored_position` per table.
    let url_rows = url_store
        .replay(&url_schema, BlockPosition(100))
        .await
        .unwrap();
    let pool_rows = pool_store
        .replay(&pool_schema, BlockPosition(100))
        .await
        .unwrap();
    assert_eq!(
        url_rows, pool_rows,
        "connect(url) and with_pool(pool) must replay identical rows"
    );

    // And both match the originally written blocks in ascending order.
    let expected: Vec<Row> = block1.into_iter().chain(block2).collect();
    assert_eq!(url_rows, expected);

    let url_last: Option<BlockPosition> =
        url_store.stored_position(&url_schema.table).await.unwrap();
    let pool_last: Option<BlockPosition> = pool_store
        .stored_position(&pool_schema.table)
        .await
        .unwrap();
    assert_eq!(
        url_last, pool_last,
        "connect(url) and with_pool(pool) must report identical stored_position"
    );
    assert_eq!(url_last, Some(BlockPosition(9)));
}

/// The injected-pool path defers connectivity errors to first store use, mirroring
/// how the eager `connect` path (see `postgres_connect_invalid_url_errors` above)
/// surfaces them at connect time. Over a *lazily-connected* pool pointing at an
/// unreachable server (port 1, nothing listening), `with_pool` constructs a store
/// synchronously and infallibly — no connect round-trip, no I/O at construction —
/// and the connectivity error surfaces only when the first store operation
/// actually touches the pool.
///
/// No Docker / container is needed — the pool is lazy and never reaches a server.
#[tokio::test]
async fn with_pool_defers_connectivity_errors_to_first_use() {
    // Connect options targeting port 1 — the same unreachable target the eager
    // `connect` prior art uses — so the URL path's errors-at-connect and the
    // injected path's errors-at-first-use are pinned against the same failure.
    let opts: PgConnectOptions = "postgres://postgres:postgres@127.0.0.1:1/postgres"
        .parse()
        .expect("parse connect options");

    // `connect_lazy_with` opens no connection now (returns `Pool`, not a future),
    // so no I/O happens at pool construction. A short acquire timeout bounds the
    // first-use failure: a connection-refused error is retried with backoff until
    // this deadline, then surfaces as `PoolTimedOut`.
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_secs(2))
        .connect_lazy_with(opts);

    // `with_pool` returns `Self` (no `.await`, no `Result`), so merely reaching
    // this binding proves construction did no connect round-trip and no I/O.
    let store = PostgresStore::with_pool(pool);

    // The connectivity error surfaces here, at the first store operation — not at
    // construction. A connection failure is not the missing-table SQLSTATE (42P01),
    // so `stored_position` propagates it as `Err` rather than misclassifying it as
    // `Ok(None)`.
    let first_use: Result<Option<BlockPosition>> = store.stored_position("t").await;
    assert!(
        first_use.is_err(),
        "first store use over an unreachable injected pool must surface the connectivity error"
    );

    // A second read fails the same way — the store fabricated no success and
    // holds no partial or half-initialised state.
    let follow_up = store.replay(&value_set_schema(), BlockPosition(100)).await;
    assert!(
        follow_up.is_err(),
        "a read over the same unreachable pool must also error, never fabricate state"
    );
}

/// Dropping every handle to a `with_pool` store leaves the caller's injected pool
/// open and usable — the store borrows the pool and never calls `.close()` on it.
/// The caller keeps its own handle (a `PgPool` clone is a handle to the same
/// shared pool); the store gets another clone, actively uses it (a `write`
/// acquires and returns a connection), and is then dropped. A `SELECT 1` on the
/// caller's retained handle must still succeed, proving the store's drop glue
/// released only its own handle and never closed the pool.
#[tokio::test]
async fn with_pool_store_leaves_injected_pool_open_on_drop() {
    let (_container, url) = start_postgres().await;

    // Caller-owned pool; the store receives a clone — a handle to the SAME pool.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();

    // Construct a store over the injected pool and actually use it (so the
    // "still usable after drop" claim is non-vacuous: the store acquired and
    // returned a connection from the borrowed pool), then drop every store handle.
    {
        let store = PostgresStore::with_pool(pool.clone());
        let schema = value_set_schema();
        store
            .write(
                &schema,
                BlockPosition(1),
                vec![Row(vec![SqlValue::Text("x".into())])],
            )
            .await
            .unwrap();
        drop(store); // the store's PgPool clone is dropped by compiler drop glue only.
    }

    // The caller's retained handle still answers queries: the store never closed
    // the borrowed pool.
    let one: i32 = sqlx::query_scalar("SELECT 1")
        .fetch_one(&pool)
        .await
        .expect("injected pool must remain open and usable after the store is dropped");
    assert_eq!(
        one, 1,
        "SELECT 1 on the still-open injected pool must return 1"
    );
}

/// Restart-resume over the same injected pool: persisting several blocks through
/// a `with_pool` store, dropping it, then rebuilding a NEW store from the SAME
/// pool yields a `replay` returning the full stored history and a
/// `stored_position` reporting the highest committed block. The store is a
/// stateless wrapper — the history lives in the database reached through the
/// borrowed pool — so a fresh store handle over the same pool sees everything the
/// first one wrote.
#[tokio::test]
async fn with_pool_store_resumes_replay_from_same_pool() {
    let (_container, url) = start_postgres().await;

    // Caller-owned pool; each store gets a clone (a handle to the same pool).
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&url)
        .await
        .unwrap();
    let schema = value_set_schema();

    // First store: persist several blocks in ascending order, then drop it.
    {
        let store = PostgresStore::with_pool(pool.clone());
        store
            .write(
                &schema,
                BlockPosition(5),
                vec![Row(vec![SqlValue::Text("a".into())])],
            )
            .await
            .unwrap();
        store
            .write(
                &schema,
                BlockPosition(9),
                vec![Row(vec![SqlValue::Text("b".into())])],
            )
            .await
            .unwrap();
        store
            .write(
                &schema,
                BlockPosition(13),
                vec![Row(vec![SqlValue::Text("c".into())])],
            )
            .await
            .unwrap();
        drop(store); // every handle to the first store is gone before we rebuild.
    }

    // Rebuild a fresh store from the SAME injected pool: it resumes the full
    // history and the highest committed watermark.
    let resumed = PostgresStore::with_pool(pool.clone());
    assert_eq!(
        resumed.replay(&schema, BlockPosition(100)).await.unwrap(),
        vec![
            Row(vec![SqlValue::Text("a".into())]),
            Row(vec![SqlValue::Text("b".into())]),
            Row(vec![SqlValue::Text("c".into())]),
        ],
        "a store rebuilt from the same pool must replay the full stored history in ascending block order"
    );
    assert_eq!(
        resumed.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(13)),
        "the rebuilt store must report the highest committed block as the resume point"
    );
}

/// Serving-layer parity tests: an archive served over PostgreSQL must produce
/// the same routes and JSON as the same archive served over SQLite, and the
/// serving connection must reject writes (also covered by the within-crate
/// `read_only_serving_pool_rejects_writes`). Gated on `serving` so
/// `cargo test --features postgres` (without `serving`) still compiles.
#[cfg(feature = "serving")]
mod serving_parity {
    use super::*;

    use std::net::SocketAddr;

    use artemis_light::ServingLayer;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::Value;
    use tower::ServiceExt;

    fn any_addr() -> SocketAddr {
        "127.0.0.1:0".parse().unwrap()
    }

    /// Write the same two-block archive to any `Store` (used to seed both the
    /// PostgreSQL and the SQLite databases identically).
    async fn seed_serving<S: Store>(store: &S) {
        // Includes a `Numeric` column: it is declared NUMERIC in SQLite and
        // TEXT in PostgreSQL, but both `/schema` responses must normalise to
        // TEXT (the type the cell decodes to). A hex-string amount stays text
        // under SQLite's NUMERIC affinity, matching PostgreSQL's TEXT column.
        let schema = TableSchema::new("evt")
            .col("name", SqlType::Text)
            .col("count", SqlType::Integer)
            .col("amount", SqlType::Numeric);
        store
            .write(
                &schema,
                BlockPosition(1),
                vec![Row(vec![
                    SqlValue::Text("alpha".into()),
                    SqlValue::Integer(10),
                    SqlValue::Text("0x2a".into()),
                ])],
            )
            .await
            .unwrap();
        store
            .write(
                &schema,
                BlockPosition(2),
                vec![Row(vec![
                    SqlValue::Text("beta".into()),
                    SqlValue::Integer(20),
                    SqlValue::Text("0x2b".into()),
                ])],
            )
            .await
            .unwrap();
    }

    /// GET `uri` from `router` and return its parsed JSON body.
    async fn get_json(router: &Router, uri: &str) -> Value {
        let response = router
            .clone()
            .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
            .await
            .unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The same logical archive served over PostgreSQL and over SQLite yields
    /// identical JSON for `/tables`, `/tables/{t}/schema`, `/tables/{t}/rows`,
    /// and `/status`.
    #[tokio::test]
    async fn pg_serving_matches_sqlite_across_routes() {
        // PostgreSQL archive, served via a postgres:// URL.
        let (_container, pg_url) = start_postgres().await;
        let pg_store = PostgresStore::connect(&pg_url).await.unwrap();
        seed_serving(&pg_store).await;
        let pg_router = ServingLayer::new(pg_url.clone(), any_addr())
            .into_router()
            .await
            .unwrap();

        // SQLite archive over a temp file (serving rejects in-memory); drop the
        // writer before opening the read-only serving pool.
        let dir = tempfile::tempdir().unwrap();
        let sqlite_url = format!("sqlite:{}", dir.path().join("events.db").to_str().unwrap());
        {
            let sqlite_store = SqliteStore::connect(&sqlite_url).await.unwrap();
            seed_serving(&sqlite_store).await;
        }
        let sqlite_router = ServingLayer::new(sqlite_url, any_addr())
            .into_router()
            .await
            .unwrap();

        for uri in [
            "/tables",
            "/tables/evt/schema",
            "/tables/evt/rows",
            "/status",
        ] {
            let pg = get_json(&pg_router, uri).await;
            let sqlite = get_json(&sqlite_router, uri).await;
            assert_eq!(pg, sqlite, "route {uri} must match across backends");
        }
    }

    /// A `_payload` cell that is not valid JSON is surfaced as the raw string,
    /// not an error, on the PostgreSQL serving path — payload-fallback parity
    /// with SQLite.
    #[tokio::test]
    async fn pg_serving_payload_non_json_falls_back_to_raw_string() {
        let (_container, url) = start_postgres().await;
        let store = PostgresStore::connect(&url).await.unwrap();
        // A table whose `_payload` column holds a non-JSON string.
        let schema = TableSchema::new("raw_evt").col("_payload", SqlType::Text);
        store
            .write(
                &schema,
                BlockPosition(1),
                vec![Row(vec![SqlValue::Text("not valid json".into())])],
            )
            .await
            .unwrap();

        let router = ServingLayer::new(url, any_addr())
            .into_router()
            .await
            .unwrap();
        let body = get_json(&router, "/tables/raw_evt/rows").await;
        assert_eq!(
            body["rows"][0]["_payload"],
            Value::String("not valid json".into()),
            "a non-JSON _payload must fall back to the raw string"
        );
    }

    /// The Postgres serving backend built from a caller-supplied pool via
    /// `ServingLayer::from_pg_pool` serves byte-identical route JSON — rows and
    /// watermarks — to a URL-constructed backend over the same data. One
    /// container backs both layers: the archive is seeded once over a separate
    /// writable connection, then read through a URL-opened read-only pool and
    /// through a caller-injected pool. The injected pool is handed over as a
    /// plain writable pool and is deliberately *not* reconfigured — no
    /// `SET default_transaction_read_only`, no session guard — because serving
    /// is SELECT-only by construction; its route output must still match the
    /// URL backend's exactly.
    #[tokio::test]
    async fn pg_serving_from_injected_pool_matches_url_backend() {
        let (_container, url) = start_postgres().await;

        // Seed the archive once over a separate writable connection, through the
        // public `Store` API (the same fixture as the SQLite-parity test).
        let writer = PostgresStore::connect(&url).await.unwrap();
        seed_serving(&writer).await;

        // URL backend: the layer opens its own read-only pool from the URL.
        let url_router = ServingLayer::new(url.clone(), any_addr())
            .into_router()
            .await
            .unwrap();

        // Injected backend: a caller-owned multi-connection pool handed to
        // `from_pg_pool`, borrowed and used as-is (no read-only session guard
        // installed on it).
        let injected_pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .unwrap();
        let injected_router = ServingLayer::from_pg_pool(injected_pool, any_addr())
            .into_router()
            .await
            .unwrap();

        // Identical JSON across every route, including the /rows payload and the
        // /status watermarks — /health also matches (both databases reachable).
        for uri in [
            "/health",
            "/tables",
            "/tables/evt/schema",
            "/tables/evt/rows",
            "/status",
        ] {
            let url_json = get_json(&url_router, uri).await;
            let injected_json = get_json(&injected_router, uri).await;
            assert_eq!(
                url_json, injected_json,
                "route {uri} must match between the URL and injected-pool backends"
            );
        }
    }
}

/// Migration parity: on PostgreSQL, an archive written under the OLD two-column
/// schema (`table_name`, `last_block` BIGINT) resumes to the SAME
/// `BlockPosition` before the first write (via the read-side `last_block`
/// fallback) and is migrated in-transaction on the first write (ADD COLUMN +
/// CAST backfill), after which the encoded `position` column decodes to the
/// same block — mirroring the SQLite migration test.
#[tokio::test]
async fn pg_pre_migration_archive_migrates_on_first_write() {
    let (_container, url) = start_postgres().await;

    // Fabricate a pre-change two-column archive with a stored `last_block = 42`.
    {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&url)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE _artemis_progress \
             (table_name TEXT PRIMARY KEY, last_block BIGINT NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO _artemis_progress (table_name, last_block) VALUES ('value_set', 42)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
    }

    let store = PostgresStore::connect(&url).await.unwrap();
    let schema = value_set_schema();

    // BEFORE the first write: the `position` column does not exist (SQLSTATE
    // 42703), so stored_position falls back to decoding `last_block`.
    assert_eq!(
        store.stored_position("value_set").await.unwrap(),
        Some(BlockPosition(42)),
        "a pre-migration Postgres archive must resume at the same block before its first write"
    );

    // The first write migrates in-transaction and re-observes block 42.
    store
        .write(
            &schema,
            BlockPosition(42),
            vec![Row(vec![SqlValue::Text("x".into())])],
        )
        .await
        .unwrap();

    // AFTER the first write: the migrated `position` column decodes to the same
    // BlockPosition.
    assert_eq!(
        store.stored_position("value_set").await.unwrap(),
        Some(BlockPosition(42)),
        "the migrated Postgres position column must decode to the same block"
    );

    // Inspect the archive directly: the `position` column now exists and holds
    // CAST(last_block AS TEXT) for the previously integer-only row.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    let (encoded,): (String,) =
        sqlx::query_as("SELECT position FROM _artemis_progress WHERE table_name = 'value_set'")
            .fetch_one(&pool)
            .await
            .unwrap();
    pool.close().await;
    assert_eq!(
        encoded, "42",
        "the Postgres migration must store CAST(last_block AS TEXT) in the position column"
    );
}
