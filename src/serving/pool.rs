//! Read-only connection pool for the serving layer.
//!
//! Distinct from [`SqliteStore`](crate::persistence::SqliteStore)'s
//! single-connection writer pool: this pool is opened `read_only(true)` so the
//! SQLite driver rejects every write (serving-layer.READONLY.1), and it does not
//! reuse the writer's pool. Under WAL, readers here observe committed snapshots
//! without blocking the writer (serving-layer.CONCURRENCY.1).

use std::str::FromStr;
use std::time::Duration;

use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

/// Open a read-only pool to `database_url` with `max_connections` connections.
///
/// `create_if_missing(false)` means a missing database file is an error rather
/// than a silently-created empty one. `:memory:` URLs are unsupported (each pool
/// would see a private empty database); they surface here as an open error.
pub async fn open_read_only_pool(
    database_url: &str,
    max_connections: u32,
) -> anyhow::Result<SqlitePool> {
    // In-memory databases are unsupported: a separate read-only pool would see a
    // private empty database, not the writer's instance. Reject fast (OQ-3).
    // Match the canonical in-memory forms precisely (`:memory:` as the whole
    // path, or a `mode=memory` URI) rather than a loose substring, so a real
    // file path that happens to contain ":memory:" is not falsely rejected.
    let path = database_url.strip_prefix("sqlite:").unwrap_or(database_url);
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
        // ...writes are rejected by the read-only connection (serving-layer.READONLY.1).
        let write = sqlx::query("INSERT INTO t (x) VALUES (1)")
            .execute(&pool)
            .await;
        assert!(write.is_err(), "read-only pool must reject writes");
    }

    #[tokio::test]
    async fn in_memory_url_is_rejected() {
        assert!(open_read_only_pool("sqlite::memory:", 1).await.is_err());
        assert!(
            open_read_only_pool("sqlite:file:foo?mode=memory&cache=shared", 1)
                .await
                .is_err()
        );
    }
}
