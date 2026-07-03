//! Read-only connection pool for the serving layer.
//!
//! Distinct from [`SqliteStore`](crate::persistence::SqliteStore)'s
//! single-connection writer pool: this pool is opened `read_only(true)` so the
//! SQLite driver rejects every write, and it does not reuse the writer's pool.
//! Under WAL, readers here observe committed snapshots without blocking the
//! writer.

use std::str::FromStr;
use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// Open a read-only pool to `database_url` with `max_connections` connections.
///
/// `create_if_missing(false)` means a missing database file is an error rather
/// than a silently-created empty one. `:memory:` URLs are rejected up front: a
/// separate read-only pool would see a private empty database, not the writer's
/// instance.
pub async fn open_read_only_pool(
    database_url: &str,
    max_connections: u32,
) -> anyhow::Result<SqlitePool> {
    // Match the canonical in-memory forms precisely (`:memory:` as the whole
    // path — after either the `sqlite:` or `sqlite://` prefix — or a
    // `mode=memory` URI) rather than a loose substring, so a real file path
    // that happens to contain ":memory:" is not falsely rejected.
    let path = database_url
        .strip_prefix("sqlite://")
        .or_else(|| database_url.strip_prefix("sqlite:"))
        .unwrap_or(database_url);
    if path == ":memory:" || path.contains("mode=memory") {
        anyhow::bail!("in-memory databases are not supported by the serving layer");
    }
    let opts = SqliteConnectOptions::from_str(database_url)?
        .read_only(true)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(max_connections)
        .connect_with(opts)
        .await?;
    Ok(pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a real file-backed SQLite DB with one table, via a writable pool.
    async fn seed_file_db() -> (tempfile::TempDir, String) {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite:{}", dir.path().join("ro.db").to_str().unwrap());
        let rw = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(
                SqliteConnectOptions::from_str(&url)
                    .unwrap()
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (x INTEGER)")
            .execute(&rw)
            .await
            .unwrap();
        rw.close().await;
        (dir, url)
    }

    #[tokio::test]
    async fn read_only_pool_rejects_writes() {
        let (_dir, url) = seed_file_db().await;
        let pool = open_read_only_pool(&url, 2).await.unwrap();
        // Reads work...
        sqlx::query("SELECT * FROM t")
            .fetch_all(&pool)
            .await
            .unwrap();
        // ...writes are rejected by the read-only connection.
        let write = sqlx::query("INSERT INTO t (x) VALUES (1)")
            .execute(&pool)
            .await;
        assert!(write.is_err(), "read-only pool must reject writes");
    }

    #[tokio::test]
    async fn in_memory_url_is_rejected() {
        // Every canonical in-memory spelling must hit the fast guard (not fall
        // through to sqlx, which would bind a fresh private empty database).
        for url in [
            "sqlite::memory:",
            "sqlite://:memory:",
            ":memory:",
            "sqlite:file:foo?mode=memory&cache=shared",
        ] {
            let err = open_read_only_pool(url, 1)
                .await
                .expect_err("in-memory URL must be rejected");
            assert!(
                err.to_string().contains("in-memory"),
                "{url} should be rejected by the in-memory guard, got: {err:#}"
            );
        }
    }
}
