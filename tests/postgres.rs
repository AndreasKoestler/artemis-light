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
