//! A [`Store`] backed by SQLite via `sqlx`.

use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions, SqliteRow,
    SqliteSynchronous,
};
use sqlx::{Arguments, Row as _, sqlite::SqliteArguments};

use super::schema::{
    BLOCK_NUMBER_COLUMN, PROGRESS_TABLE, Row, SqlType, SqlValue, TableSchema, quote_ident,
};
use super::sql;
use super::store::Store;

/// A SQLite-backed [`Store`].
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    /// Open (creating if missing) a SQLite database at `url`.
    ///
    /// Use `"sqlite::memory:"` for an ephemeral in-memory database. A single
    /// connection is used so an in-memory database is shared across calls and
    /// every write sees a consistent view.
    ///
    /// File databases run in WAL journal mode with `synchronous = NORMAL` and
    /// a 5s busy timeout: the default rollback journal answers any concurrent
    /// access with an immediate `SQLITE_BUSY`, and a single failed write
    /// permanently halts persistence (by design, to keep the stored height a
    /// gap-free prefix) — so a stray reader must wait, not kill the archive.
    /// In-memory databases ignore the journal mode.
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }
}

/// Bind a [`SqlValue`] onto a `sqlx` argument list.
fn bind_value(args: &mut SqliteArguments<'_>, value: &SqlValue) -> Result<()> {
    match value {
        SqlValue::Integer(i) => args.add(*i),
        SqlValue::Real(r) => args.add(*r),
        SqlValue::Text(s) => args.add(s.clone()),
        SqlValue::Blob(b) => args.add(b.clone()),
        SqlValue::Null => args.add(Option::<i64>::None),
    }
    .map_err(|e| anyhow::anyhow!("failed to bind value: {e}"))
}

/// Decode column `idx` of `row` into a [`SqlValue`] per its declared type.
fn decode_value(row: &SqliteRow, idx: usize, ty: SqlType) -> Result<SqlValue> {
    let value = match ty {
        SqlType::Integer => SqlValue::Integer(row.try_get::<i64, _>(idx)?),
        SqlType::Real => SqlValue::Real(row.try_get::<f64, _>(idx)?),
        SqlType::Text | SqlType::Numeric => SqlValue::Text(row.try_get::<String, _>(idx)?),
        SqlType::Blob => SqlValue::Blob(row.try_get::<Vec<u8>, _>(idx)?),
    };
    Ok(value)
}

#[async_trait]
impl Store for SqliteStore {
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Progress table tracking the last processed block per event table.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} \
             (table_name TEXT PRIMARY KEY, last_block INTEGER NOT NULL)"
        ))
        .execute(&mut *tx)
        .await?;

        // Create the table on demand: an implicit block_number column plus one
        // column per event field.
        let mut defs = vec![format!("{BLOCK_NUMBER_COLUMN} INTEGER NOT NULL")];
        for c in &schema.columns {
            defs.push(format!("{} {}", quote_ident(&c.name), c.ty.sql()));
        }
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quote_ident(&schema.table),
            defs.join(", ")
        );
        sqlx::query(&create).execute(&mut *tx).await?;

        // Insert every row in the block.
        let col_names = sql::insert_column_names(schema);
        let placeholders = vec!["?"; col_names.len()].join(", ");
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&schema.table),
            col_names.join(", "),
            placeholders
        );

        for row in &rows {
            sql::check_row_shape(schema, row)?;
            let mut args = SqliteArguments::default();
            bind_value(&mut args, &SqlValue::Integer(block as i64))?;
            for value in &row.0 {
                bind_value(&mut args, value)?;
            }
            sqlx::query_with(&insert, args).execute(&mut *tx).await?;
        }

        // Advance the last processed block in the same transaction.
        sqlx::query(&format!(
            "INSERT INTO {PROGRESS_TABLE} (table_name, last_block) VALUES (?, ?) \
             ON CONFLICT(table_name) DO UPDATE SET last_block = MAX(last_block, excluded.last_block)"
        ))
        .bind(&schema.table)
        .bind(block as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        let query = sql::last_block_query("?");
        let row = match sqlx::query(&query)
            .bind(table)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            // No progress table means nothing has ever been written.
            Err(sqlx::Error::Database(e)) if e.message().contains("no such table") => None,
            Err(e) => return Err(e.into()),
        };
        Ok(row.map(|r| r.get::<i64, _>(0) as u64))
    }

    async fn replay(&self, schema: &TableSchema, to: u64) -> Result<Vec<Row>> {
        let query = sql::replay_query(schema, "?", "rowid");

        let sqlx_rows = match sqlx::query(&query)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
        {
            Ok(rows) => rows,
            // A missing table means nothing has been stored yet.
            Err(sqlx::Error::Database(e)) if e.message().contains("no such table") => {
                return Ok(Vec::new());
            }
            Err(e) => return Err(e.into()),
        };

        sql::collect_rows(&sqlx_rows, schema, decode_value)
    }
}
