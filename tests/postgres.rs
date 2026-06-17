//! Integration tests for the PostgreSQL-backed [`Store`], gated behind the
//! `postgres` feature so the default `cargo test` needs neither Docker nor a
//! running PostgreSQL (postgres-store.TESTING.1/.2). Each test provisions a
//! throwaway PostgreSQL container via testcontainers.
#![cfg(feature = "postgres")]

use artemis_light::persistence::{PostgresStore, Row, SqlType, SqlValue, Store, TableSchema};
use testcontainers::ContainerAsync;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

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
