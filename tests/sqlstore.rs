//! Integration tests for the generic SQL store's write/read paths that need a
//! real (in-memory or temp-file) SQLite database but no Docker: chunked
//! multi-row inserts, and loud-but-non-panicking failures on a foreign or
//! corrupted `_artemis_progress` table.

use std::str::FromStr;

use anyhow::Result;
use artemis_light::persistence::{
    BlockPosition, Row, SqlType, SqlValue, SqliteStore, Store, TableSchema,
};

/// A throwaway file-backed SQLite database seeded with `setup` statements
/// through a plain sqlx pool, returned as its URL (kept alive by the tempdir).
async fn seeded_db(setup: &[&str]) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    let url = format!("sqlite:{}", dir.path().join("archive.db").display());
    let opts = sqlx::sqlite::SqliteConnectOptions::from_str(&url)
        .unwrap()
        .create_if_missing(true);
    let pool = sqlx::sqlite::SqlitePool::connect_with(opts).await.unwrap();
    for sql in setup {
        sqlx::query(sql).execute(&pool).await.unwrap();
    }
    pool.close().await;
    (dir, url)
}

// A foreign pre-migration progress table holding a NULL
// `last_block` must surface as an error from `stored_position`, not a panic
// inside library code.
#[tokio::test]
async fn stored_position_errs_on_a_null_last_block_cell() {
    let (_dir, url) = seeded_db(&[
        "CREATE TABLE _artemis_progress (table_name TEXT PRIMARY KEY, last_block INTEGER)",
        "INSERT INTO _artemis_progress (table_name, last_block) VALUES ('t', NULL)",
    ])
    .await;

    let store = SqliteStore::connect(&url).await.unwrap();
    let result: Result<Option<BlockPosition>> = store.stored_position("t").await;
    assert!(result.is_err(), "a NULL last_block must err, not panic");
}

// A corrupted `position` cell (a non-textual BLOB) must surface as
// an error from `stored_position`, not a panic.
#[tokio::test]
async fn stored_position_errs_on_a_wrong_typed_position_cell() {
    let (_dir, url) = seeded_db(&[
        "CREATE TABLE _artemis_progress \
         (table_name TEXT PRIMARY KEY, last_block INTEGER, position TEXT)",
        "INSERT INTO _artemis_progress (table_name, last_block, position) \
         VALUES ('t', 1, X'FFFE')",
    ])
    .await;

    let store = SqliteStore::connect(&url).await.unwrap();
    let result: Result<Option<BlockPosition>> = store.stored_position("t").await;
    assert!(
        result.is_err(),
        "a non-text position cell must err, not panic"
    );
}

// A write large enough to span several insert chunks replays back complete
// and in order — the chunked multi-row insert is behaviourally identical to
// the old one-round-trip-per-row insert.
#[tokio::test]
async fn a_write_crossing_chunk_boundaries_replays_in_order() {
    let store = SqliteStore::connect("sqlite::memory:").await.unwrap();
    let schema = TableSchema::new("t").col("v", SqlType::Text);

    // 2 bind parameters per row (block_number + v) under the 999-parameter
    // cap gives 499 rows per chunk: 1100 rows spans two full chunks plus a
    // partial trailing one.
    let rows: Vec<Row> = (0..1100)
        .map(|i| Row(vec![SqlValue::Text(format!("r{i:04}"))]))
        .collect();
    store
        .write(&schema, BlockPosition(7), rows.clone())
        .await
        .unwrap();

    let replayed = store.replay(&schema, BlockPosition(7)).await.unwrap();
    assert_eq!(replayed, rows);
    assert_eq!(
        store.stored_position(&schema.table).await.unwrap(),
        Some(BlockPosition(7))
    );
}
