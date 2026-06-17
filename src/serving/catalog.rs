//! SQLite catalog introspection: table listing, column schema, and the
//! validated-table guard. There is no runtime table catalogue in the
//! persistence layer, so the serving layer reads SQLite's own `sqlite_master`
//! and `PRAGMA table_info` (serving-layer.TABLES.1/.2/.3).

use sqlx::{Row, SqlitePool};

// Shared with the writer so the serving layer can never diverge on the internal
// table name or identifier quoting: `PROGRESS_TABLE` is the bookkeeping table
// that must stay hidden from `/tables`, and `quote_ident` matches the writer's
// quoting exactly.
use crate::persistence::{PROGRESS_TABLE, quote_ident};

/// List the persisted event tables: every `sqlite_master` table except the
/// internal progress table and SQLite's own `sqlite_%` tables, sorted ascending.
pub(crate) async fn list_tables(pool: &SqlitePool) -> anyhow::Result<Vec<String>> {
    let rows = sqlx::query(
        "SELECT name FROM sqlite_master \
         WHERE type = 'table' AND name <> ? AND name NOT LIKE 'sqlite_%' \
         ORDER BY name",
    )
    .bind(PROGRESS_TABLE)
    .fetch_all(pool)
    .await?;
    Ok(rows.iter().map(|r| r.get::<String, _>("name")).collect())
}

/// The validated-table guard: true only when `table` is a real event table in
/// the catalog (not internal, not `sqlite_%`). Callers MUST check this before
/// interpolating a table name into any SQL (serving-layer.TABLES.3 / injection
/// guard).
///
/// There is a check-then-use gap between this guard and the subsequent query,
/// but it is benign: the persistence layer only ever *creates* event tables,
/// never drops them, so a validated name cannot disappear mid-request. A dropped
/// table would at worst surface as a `Database` (500), never as injection.
pub(crate) async fn table_exists(pool: &SqlitePool, table: &str) -> anyhow::Result<bool> {
    let row = sqlx::query(
        "SELECT 1 FROM sqlite_master \
         WHERE type = 'table' AND name = ? AND name <> ? AND name NOT LIKE 'sqlite_%'",
    )
    .bind(table)
    .bind(PROGRESS_TABLE)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}

/// Normalise a SQLite declared column type to the canonical serving type
/// keyword, so `/schema` responses are identical across backends
/// (postgres-store.SERVE.3). `NUMERIC` columns decode to text on both backends
/// (a `Numeric` value round-trips as `SqlValue::Text`), and the PostgreSQL
/// store stores them as `TEXT`, so they report `TEXT` here rather than the raw
/// `NUMERIC` affinity declared in `CREATE TABLE`. The remaining writer types
/// (`INTEGER`/`REAL`/`TEXT`/`BLOB`) are already canonical.
fn normalize_type(declared: &str) -> String {
    match declared.to_ascii_uppercase().as_str() {
        "NUMERIC" => "TEXT".to_string(),
        other => other.to_string(),
    }
}

/// Column `(name, type)` pairs for `table`, in declared order, from
/// `PRAGMA table_info` (serving-layer.TABLES.2). `table` MUST have passed
/// [`table_exists`] first; it is quoted defensively before interpolation.
pub(crate) async fn table_columns(
    pool: &SqlitePool,
    table: &str,
) -> anyhow::Result<Vec<(String, String)>> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    Ok(rows
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("name"),
                normalize_type(&r.get::<String, _>("type")),
            )
        })
        .collect())
}

/// Per-table watermarks `(table_name, last_block)` from `_artemis_progress`,
/// sorted by table name (serving-layer.STATUS.1). A missing progress table
/// (nothing written yet) yields an empty list rather than an error.
pub(crate) async fn table_watermarks(pool: &SqlitePool) -> anyhow::Result<Vec<(String, i64)>> {
    let rows = match sqlx::query(
        "SELECT table_name, last_block FROM _artemis_progress ORDER BY table_name",
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => rows,
        Err(sqlx::Error::Database(e)) if e.message().contains("no such table") => {
            return Ok(Vec::new());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    async fn mem_pool() -> SqlitePool {
        SqlitePool::connect("sqlite::memory:").await.unwrap()
    }

    #[tokio::test]
    async fn excludes_internal_tables_and_sorts() {
        let pool = mem_pool().await;
        sqlx::query("CREATE TABLE _artemis_progress (table_name TEXT PRIMARY KEY, last_block INTEGER NOT NULL)")
            .execute(&pool).await.unwrap();
        sqlx::query(
            "CREATE TABLE value_set (block_number INTEGER NOT NULL, value INTEGER, _payload TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("CREATE TABLE transfer (block_number INTEGER NOT NULL, _payload TEXT)")
            .execute(&pool)
            .await
            .unwrap();

        let tables = list_tables(&pool).await.unwrap();
        assert_eq!(
            tables,
            vec!["transfer".to_string(), "value_set".to_string()]
        );
    }

    #[tokio::test]
    async fn empty_database_yields_no_tables() {
        let pool = mem_pool().await;
        assert!(list_tables(&pool).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn table_exists_rejects_internal_and_absent() {
        let pool = mem_pool().await;
        sqlx::query("CREATE TABLE _artemis_progress (table_name TEXT PRIMARY KEY, last_block INTEGER NOT NULL)")
            .execute(&pool).await.unwrap();
        sqlx::query(
            "CREATE TABLE value_set (block_number INTEGER NOT NULL, value INTEGER, _payload TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        assert!(table_exists(&pool, "value_set").await.unwrap());
        assert!(!table_exists(&pool, "_artemis_progress").await.unwrap());
        assert!(!table_exists(&pool, "missing").await.unwrap());
    }

    #[test]
    fn quote_ident_doubles_interior_quotes() {
        assert_eq!(quote_ident("value_set"), "\"value_set\"");
        assert_eq!(quote_ident("a\"b"), "\"a\"\"b\"");
    }

    #[tokio::test]
    async fn table_columns_reports_numeric_as_text() {
        // A `Numeric` column decodes to text and the PostgreSQL store stores it
        // as TEXT, so the SQLite `/schema` path must report TEXT too — otherwise
        // the two backends would disagree on the column type (SERVE.3).
        let pool = mem_pool().await;
        sqlx::query(
            "CREATE TABLE evt (block_number INTEGER NOT NULL, amount NUMERIC, \
             note TEXT, count INTEGER, raw BLOB, ratio REAL)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let cols = table_columns(&pool, "evt").await.unwrap();
        assert_eq!(
            cols,
            vec![
                ("block_number".to_string(), "INTEGER".to_string()),
                ("amount".to_string(), "TEXT".to_string()),
                ("note".to_string(), "TEXT".to_string()),
                ("count".to_string(), "INTEGER".to_string()),
                ("raw".to_string(), "BLOB".to_string()),
                ("ratio".to_string(), "REAL".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn watermarks_sorted_and_empty_when_absent() {
        let pool = mem_pool().await;
        // No _artemis_progress yet → empty, not an error.
        assert!(table_watermarks(&pool).await.unwrap().is_empty());

        sqlx::query("CREATE TABLE _artemis_progress (table_name TEXT PRIMARY KEY, last_block INTEGER NOT NULL)")
            .execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO _artemis_progress VALUES ('value_set', 100), ('transfer', 50)")
            .execute(&pool)
            .await
            .unwrap();

        let wms = table_watermarks(&pool).await.unwrap();
        assert_eq!(
            wms,
            vec![("transfer".to_string(), 50), ("value_set".to_string(), 100)]
        );
    }
}
