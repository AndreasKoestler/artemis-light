//! Storage-backend abstraction for the read-only serving layer.
//!
//! The route handlers are written against the [`ServingBackend`] trait rather
//! than a concrete pool, so the same routes and JSON responses can be served
//! over different storage engines selected by URL scheme. [`SqliteBackend`] is
//! the SQLite implementation; it delegates to the existing SQLite
//! catalog/rows/json helpers.

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
/// SQLite catalog/rows helpers.
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

/// A [`ServingBackend`] backed by a read-only PostgreSQL pool. Introspects via
/// `information_schema`, decodes `PgRow` cells to the same JSON shape as the
/// SQLite backend, and enforces a read-only session so the serving layer
/// cannot mutate the archive. Reads pin the `public` schema; the writer
/// intentionally writes unqualified via the session `search_path` (see
/// [`PostgresStore`](crate::persistence::PostgresStore)).
/// Compiled only when both `serving` and `postgres` are enabled.
#[cfg(feature = "postgres")]
pub(crate) use pg::PgBackend;

#[cfg(feature = "postgres")]
mod pg {
    use async_trait::async_trait;
    use serde_json::{Map, Value};
    use sqlx::Row as _;
    use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};

    use super::super::json;
    use super::super::rows::Bounds;
    use super::ServingBackend;
    use crate::persistence::{Dialect, PROGRESS_TABLE, PgDialect, range_query};

    /// The `information_schema` existence probe behind
    /// [`ServingBackend::table_exists`]. Filters to `BASE TABLE` exactly like
    /// `list_tables`, so a view can never pass the guard while being invisible
    /// in `/tables`.
    const TABLE_EXISTS_SQL: &str = "SELECT 1 FROM information_schema.tables \
         WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
         AND table_name = $1 AND table_name <> $2";

    /// The watermark query behind [`ServingBackend::watermarks`],
    /// schema-qualified like [`range_query_public`].
    fn watermarks_sql() -> String {
        format!("SELECT table_name, last_block FROM public.{PROGRESS_TABLE} ORDER BY table_name")
    }

    /// The paged range query behind [`ServingBackend::query_rows`]: the shared
    /// [`range_query`] shape with the table reference pinned to the `public`
    /// schema. Introspection filters `table_schema = 'public'`, so the data
    /// read must resolve the same relation regardless of the pool's
    /// `search_path` (an injected pool may front another schema). Note the
    /// writer intentionally writes unqualified via the session `search_path`
    /// (documented on `PostgresStore`); pinning here is read-side only.
    fn range_query_public(table: &str) -> String {
        range_query(table, Some("public"), &PgDialect)
    }

    /// A read-only PostgreSQL serving backend.
    pub(crate) struct PgBackend {
        pool: PgPool,
    }

    impl PgBackend {
        /// Open a read-only pool to `url`. Every pooled connection runs
        /// `SET default_transaction_read_only = on` so writes through the
        /// serving layer are rejected at the session level.
        pub(crate) async fn connect(url: &str, max_connections: u32) -> anyhow::Result<Self> {
            let pool = PgPoolOptions::new()
                .max_connections(max_connections)
                .after_connect(|conn, _meta| {
                    Box::pin(async move {
                        sqlx::query("SET default_transaction_read_only = on")
                            .execute(&mut *conn)
                            .await?;
                        Ok(())
                    })
                })
                .connect(url)
                .await?;
            Ok(Self { pool })
        }

        /// Wrap a caller-supplied pool as a read-only serving backend, the
        /// serving twin of
        /// [`PostgresStore::with_pool`](crate::persistence::PostgresStore).
        /// Unlike [`connect`](Self::connect), this installs **no**
        /// `after_connect` hook (no `SET default_transaction_read_only = on`)
        /// and does no connect round-trip: the borrowed pool is used as-is and
        /// never reconfigured.
        pub(crate) fn with_pool(pool: PgPool) -> Self {
            Self { pool }
        }

        #[cfg(test)]
        pub(crate) fn pool(&self) -> &PgPool {
            &self.pool
        }
    }

    /// Normalise a PostgreSQL `information_schema` `data_type` to the same
    /// column-type keyword the SQLite backend reports, so `/schema` responses
    /// match across backends and the keyword drives cell decoding uniformly.
    /// `Numeric` columns are stored as `TEXT`, so a PostgreSQL-served Numeric
    /// column reports `TEXT` here.
    fn normalize_type(data_type: &str) -> String {
        match data_type {
            "bigint" => "INTEGER".to_string(),
            "double precision" => "REAL".to_string(),
            "bytea" => "BLOB".to_string(),
            "text" => "TEXT".to_string(),
            other => other.to_ascii_uppercase(),
        }
    }

    // Extract typed, nullable cells from a `PgRow` so the shared
    // [`json::row_to_json`] decoder renders PostgreSQL rows identically to
    // SQLite. The decode *rule* lives in `json`; the macro supplies only the
    // per-type extraction, shared with `SqliteRow`.
    json::impl_cell!(PgRow);

    #[async_trait]
    impl ServingBackend for PgBackend {
        async fn health(&self) -> anyhow::Result<()> {
            sqlx::query("SELECT 1").fetch_one(&self.pool).await?;
            Ok(())
        }

        async fn list_tables(&self) -> anyhow::Result<Vec<String>> {
            let rows = sqlx::query(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_type = 'BASE TABLE' \
                 AND table_name <> $1 ORDER BY table_name",
            )
            .bind(PROGRESS_TABLE)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows
                .iter()
                .map(|r| r.get::<String, _>("table_name"))
                .collect())
        }

        async fn table_exists(&self, table: &str) -> anyhow::Result<bool> {
            let row = sqlx::query(TABLE_EXISTS_SQL)
                .bind(table)
                .bind(PROGRESS_TABLE)
                .fetch_optional(&self.pool)
                .await?;
            Ok(row.is_some())
        }

        async fn table_columns(&self, table: &str) -> anyhow::Result<Vec<(String, String)>> {
            let rows = sqlx::query(
                "SELECT column_name, data_type FROM information_schema.columns \
                 WHERE table_schema = 'public' AND table_name = $1 ORDER BY ordinal_position",
            )
            .bind(table)
            .fetch_all(&self.pool)
            .await?;
            Ok(rows
                .iter()
                .map(|r| {
                    (
                        r.get::<String, _>("column_name"),
                        normalize_type(&r.get::<String, _>("data_type")),
                    )
                })
                .collect())
        }

        async fn query_rows(
            &self,
            table: &str,
            bounds: &Bounds,
        ) -> anyhow::Result<Vec<Map<String, Value>>> {
            let columns = self.table_columns(table).await?;
            let sql = range_query_public(table);
            let rows = sqlx::query(&sql)
                .bind(bounds.from_block as i64)
                .bind(bounds.to_block as i64)
                .bind(bounds.limit as i64)
                .bind(bounds.offset as i64)
                .fetch_all(&self.pool)
                .await?;
            rows.iter()
                .map(|r| json::row_to_json(r, &columns))
                .collect()
        }

        async fn watermarks(&self) -> anyhow::Result<Vec<(String, i64)>> {
            let rows = match sqlx::query(&watermarks_sql()).fetch_all(&self.pool).await {
                Ok(rows) => rows,
                // Nothing written yet: the progress table does not exist.
                Err(e) if PgDialect.is_undefined_table(&e) => return Ok(Vec::new()),
                Err(e) => return Err(e.into()),
            };
            Ok(rows
                .iter()
                .map(|r| {
                    (
                        r.get::<String, _>("table_name"),
                        r.get::<i64, _>("last_block"),
                    )
                })
                .collect())
        }
    }

    /// SQL-string tests for the PostgreSQL backend's queries; no live server
    /// needed (the Docker-backed behavioural tests live in `pg_backend_tests`).
    #[cfg(test)]
    mod sql_tests {
        use super::*;

        /// The existence guard must apply the same `BASE TABLE` filter as
        /// `list_tables`, so a view can never pass the guard while being
        /// invisible in `/tables` (and then break on the `ctid` tie-breaker).
        #[test]
        fn table_exists_filters_to_base_tables() {
            assert!(
                TABLE_EXISTS_SQL.contains("table_type = 'BASE TABLE'"),
                "table_exists must filter to BASE TABLE like list_tables, got: {TABLE_EXISTS_SQL}"
            );
        }

        /// Data queries must pin the `public` schema so an injected pool whose
        /// `search_path` fronts another schema still reads the relation that
        /// introspection (which filters `table_schema = 'public'`) validated.
        #[test]
        fn data_queries_are_schema_qualified() {
            let sql = range_query_public("transfer");
            assert!(
                sql.contains("FROM \"public\".\"transfer\""),
                "range query must read \"public\".\"transfer\", got: {sql}"
            );
            // The table name stays quoted; only the FROM clause is qualified.
            let quoted = range_query_public("a\"b");
            assert!(
                quoted.contains("FROM \"public\".\"a\"\"b\""),
                "quoting must survive qualification, got: {quoted}"
            );
            let wm = watermarks_sql();
            assert!(
                wm.contains(&format!("FROM public.{PROGRESS_TABLE}")),
                "watermark query must read public.{PROGRESS_TABLE}, got: {wm}"
            );
        }
    }
}

#[cfg(all(test, feature = "postgres"))]
mod pg_backend_tests {
    use super::super::rows::Bounds;
    use super::ServingBackend;
    use super::pg::PgBackend;
    use testcontainers_modules::postgres::Postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    /// Every connection in the PostgreSQL serving pool is read-only, so a write
    /// issued through it fails at the session level.
    #[tokio::test]
    async fn read_only_serving_pool_rejects_writes() {
        let node = Postgres::default()
            .start()
            .await
            .expect("start postgres container");
        let port = node.get_host_port_ipv4(5432).await.expect("map port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

        let backend = PgBackend::connect(&url, 2).await.expect("connect");
        let result = sqlx::query("CREATE TABLE rejected (a bigint)")
            .execute(backend.pool())
            .await;
        assert!(
            result.is_err(),
            "the read-only serving pool must reject writes"
        );
    }

    /// The PostgreSQL backend decodes each column type to the same JSON shape as
    /// the SQLite backend (the shared `json::row_to_json` rule, exercised here
    /// through `PgRow`): integer → number, real → number, bytea → `0x`-hex, NULL
    /// → null, and `_payload` → nested JSON. The SQLite twin is
    /// `serving::json::tests::converts_cells_payload_and_blob`.
    #[tokio::test]
    async fn pg_decodes_each_column_type_to_the_shared_json_shape() {
        let node = Postgres::default()
            .start()
            .await
            .expect("start postgres container");
        let port = node.get_host_port_ipv4(5432).await.expect("map port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");

        // The serving pool is read-only, so set the fixture up over a separate
        // writable connection.
        let setup = sqlx::PgPool::connect(&url).await.expect("writable connect");
        sqlx::query(
            "CREATE TABLE decode_t (block_number bigint, n bigint, ratio double precision, \
             raw bytea, missing bigint, _payload text)",
        )
        .execute(&setup)
        .await
        .expect("create table");
        sqlx::query(
            "INSERT INTO decode_t (block_number, n, ratio, raw, missing, _payload) \
             VALUES (100, 7, 1.5, '\\x00ff', NULL, '{\"value\":\"7\"}')",
        )
        .execute(&setup)
        .await
        .expect("insert row");

        let backend = PgBackend::connect(&url, 2).await.expect("connect");
        let bounds = Bounds {
            from_block: 0,
            to_block: 1_000,
            limit: 10,
            offset: 0,
        };
        let rows = backend
            .query_rows("decode_t", &bounds)
            .await
            .expect("query_rows");

        assert_eq!(rows.len(), 1);
        let obj = &rows[0];
        assert_eq!(obj["block_number"], serde_json::json!(100));
        assert_eq!(obj["n"], serde_json::json!(7));
        assert_eq!(obj["ratio"], serde_json::json!(1.5));
        assert_eq!(obj["raw"], serde_json::json!("0x00ff"));
        assert_eq!(obj["missing"], serde_json::Value::Null);
        // `_payload` parsed into nested JSON, not echoed as a string.
        assert_eq!(obj["_payload"], serde_json::json!({ "value": "7" }));
    }
}
