//! Storage-backend abstraction for the read-only serving layer.
//!
//! The route handlers are written against the [`ServingBackend`] trait rather
//! than a concrete pool, so the same routes and JSON responses can be served
//! over different storage engines selected by URL scheme
//! (postgres-store.SERVE.1/.2/.3). [`SqliteBackend`] is the SQLite
//! implementation; it delegates to the existing SQLite catalog/rows/json
//! helpers, so SQLite serving behaviour is unchanged (postgres-store.FEATURE.2).

use async_trait::async_trait;
use serde_json::{Map, Value};
use sqlx::SqlitePool;

use super::catalog;
use super::rows::{self, Bounds};

/// Read-only operations the serving routes need from a storage backend. Each
/// method returns already-decoded data so the route handlers and JSON response
/// types stay backend-agnostic.
#[async_trait]
pub(crate) trait ServingBackend: Send + Sync {
    /// Liveness probe: `Ok(())` when the database is reachable.
    async fn health(&self) -> anyhow::Result<()>;
    /// The persisted event tables (excluding internal bookkeeping), sorted.
    async fn list_tables(&self) -> anyhow::Result<Vec<String>>;
    /// Whether `table` is a real event table — the SQL-injection guard callers
    /// MUST check before interpolating a table name.
    async fn table_exists(&self, table: &str) -> anyhow::Result<bool>;
    /// Column `(name, type)` pairs for `table`, in declared order.
    async fn table_columns(&self, table: &str) -> anyhow::Result<Vec<(String, String)>>;
    /// A page of rows for a validated `table`, decoded to JSON objects in
    /// ascending block order.
    async fn query_rows(
        &self,
        table: &str,
        bounds: &Bounds,
    ) -> anyhow::Result<Vec<Map<String, Value>>>;
    /// Per-table `(table_name, last_block)` watermarks, sorted by table name.
    async fn watermarks(&self) -> anyhow::Result<Vec<(String, i64)>>;
}

/// A [`ServingBackend`] backed by a read-only SQLite pool, delegating to the
/// SQLite catalog/rows helpers so behaviour is byte-for-byte unchanged.
pub(crate) struct SqliteBackend {
    pool: SqlitePool,
}

impl SqliteBackend {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl ServingBackend for SqliteBackend {
    async fn health(&self) -> anyhow::Result<()> {
        sqlx::query("SELECT 1").fetch_one(&self.pool).await?;
        Ok(())
    }

    async fn list_tables(&self) -> anyhow::Result<Vec<String>> {
        catalog::list_tables(&self.pool).await
    }

    async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
        catalog::table_exists(&self.pool, table).await
    }

    async fn table_columns(&self, table: &str) -> anyhow::Result<Vec<(String, String)>> {
        catalog::table_columns(&self.pool, table).await
    }

    async fn query_rows(
        &self,
        table: &str,
        bounds: &Bounds,
    ) -> anyhow::Result<Vec<Map<String, Value>>> {
        // The handler validated `table` via `table_exists` first; fetch its
        // columns to drive cell decoding, then run the paged range query.
        let columns = catalog::table_columns(&self.pool, table).await?;
        rows::query_rows(&self.pool, table, &columns, bounds).await
    }

    async fn watermarks(&self) -> anyhow::Result<Vec<(String, i64)>> {
        catalog::table_watermarks(&self.pool).await
    }
}
