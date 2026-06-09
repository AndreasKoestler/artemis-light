//! A [`Store`] backed by SQLite via `sqlx`.

use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Arguments, Row as _, sqlite::SqliteArguments};

use super::schema::{Row, SqlType, SqlValue, TableSchema};
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
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url)?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }
}

/// Quote a SQL identifier (table/column name) for inclusion in a statement.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
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
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS _artemis_progress \
             (table_name TEXT PRIMARY KEY, last_block INTEGER NOT NULL)",
        )
        .execute(&mut *tx)
        .await?;

        // Create the table on demand: an implicit block_number column plus one
        // column per event field.
        let mut defs = vec!["block_number INTEGER NOT NULL".to_string()];
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
        let mut col_names = vec!["block_number".to_string()];
        col_names.extend(schema.columns.iter().map(|c| quote_ident(&c.name)));
        let placeholders = vec!["?"; col_names.len()].join(", ");
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&schema.table),
            col_names.join(", "),
            placeholders
        );

        for row in &rows {
            // Guard against shape mismatches: sqlx silently binds NULL for a
            // short argument list, so a row that does not match the schema
            // would corrupt the table rather than error. Bail (rolling the
            // transaction back) instead.
            if row.0.len() != schema.columns.len() {
                anyhow::bail!(
                    "row has {} values but table {:?} has {} columns",
                    row.0.len(),
                    schema.table,
                    schema.columns.len()
                );
            }
            let mut args = SqliteArguments::default();
            bind_value(&mut args, &SqlValue::Integer(block as i64))?;
            for value in &row.0 {
                bind_value(&mut args, value)?;
            }
            sqlx::query_with(&insert, args).execute(&mut *tx).await?;
        }

        // Advance the last processed block in the same transaction.
        sqlx::query(
            "INSERT INTO _artemis_progress (table_name, last_block) VALUES (?, ?) \
             ON CONFLICT(table_name) DO UPDATE SET last_block = MAX(last_block, excluded.last_block)",
        )
        .bind(&schema.table)
        .bind(block as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        let row = match sqlx::query("SELECT last_block FROM _artemis_progress WHERE table_name = ?")
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
        let select_cols = schema
            .columns
            .iter()
            .map(|c| quote_ident(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT {} FROM {} WHERE block_number <= ? ORDER BY block_number ASC, rowid ASC",
            select_cols,
            quote_ident(&schema.table)
        );

        let sqlx_rows = match sqlx::query(&sql)
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

        let mut out = Vec::with_capacity(sqlx_rows.len());
        for r in &sqlx_rows {
            let mut values = Vec::with_capacity(schema.columns.len());
            for (idx, c) in schema.columns.iter().enumerate() {
                values.push(decode_value(r, idx, c.ty)?);
            }
            out.push(Row(values));
        }
        Ok(out)
    }
}
