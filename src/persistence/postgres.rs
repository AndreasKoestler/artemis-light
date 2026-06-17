//! A [`Store`] backed by PostgreSQL via `sqlx`.
//!
//! Mirrors [`SqliteStore`](super::SqliteStore) method-for-method against the
//! same [`Store`] contract (postgres-store.PGSTORE.8); the differences are
//! dialect-only: `$N` positional placeholders, `GREATEST(...)` for the
//! monotonic watermark upsert, SQLSTATE `42P01` (undefined_table) for
//! missing-table classification, and `ctid` as the stable intra-block order
//! key. Compiled only under the `postgres` feature (postgres-store.FEATURE.1).

use std::str::FromStr;

use anyhow::Result;
use async_trait::async_trait;
use sqlx::postgres::{PgArguments, PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::{Arguments, Row as _};

use super::schema::{
    BLOCK_NUMBER_COLUMN, PROGRESS_TABLE, Row, SqlType, SqlValue, TableSchema, quote_ident,
};
use super::store::Store;

/// A PostgreSQL-backed [`Store`].
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    /// Open a connection pool to the PostgreSQL database at `url` (a
    /// `postgres://` / `postgresql://` URL) (postgres-store.PGSTORE.1).
    ///
    /// A single writer connection (`max_connections(1)`) mirrors
    /// [`SqliteStore`](super::SqliteStore): the persistence pipeline has one
    /// writer per archive (postgres-store.DURABILITY.3), and serializing writes
    /// keeps the stored height a gap-free prefix even though PostgreSQL could
    /// otherwise admit concurrent writers. An unreachable or invalid URL
    /// surfaces as an error here rather than a half-open store
    /// (postgres-store.PGSTORE.1-1).
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = PgConnectOptions::from_str(url)?;
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        Ok(Self { pool })
    }
}

/// The PostgreSQL column type each [`SqlType`] maps to, applied consistently
/// across create, write, and replay (postgres-store.TYPES.1).
///
/// `Numeric` maps to `TEXT` (not PostgreSQL `NUMERIC`) on purpose: SqliteStore
/// decodes a `Numeric` column back as [`SqlValue::Text`], so a `TEXT` column
/// makes PostgreSQL replay produce the identical value (postgres-store.PARITY.1)
/// without pulling in a decimal dependency.
fn pg_column_type(ty: SqlType) -> &'static str {
    match ty {
        SqlType::Integer => "BIGINT",
        SqlType::Real => "DOUBLE PRECISION",
        SqlType::Text => "TEXT",
        SqlType::Blob => "BYTEA",
        SqlType::Numeric => "TEXT",
    }
}

/// Bind a [`SqlValue`] onto a `sqlx` PostgreSQL argument list.
fn bind_value(args: &mut PgArguments, value: &SqlValue) -> Result<()> {
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
///
/// `Numeric` decodes as `String` (same arm as `Text`), matching SqliteStore so
/// replay round-trips to logically identical rows (postgres-store.PARITY.1).
fn decode_value(row: &PgRow, idx: usize, ty: SqlType) -> Result<SqlValue> {
    let value = match ty {
        SqlType::Integer => SqlValue::Integer(row.try_get::<i64, _>(idx)?),
        SqlType::Real => SqlValue::Real(row.try_get::<f64, _>(idx)?),
        SqlType::Text | SqlType::Numeric => SqlValue::Text(row.try_get::<String, _>(idx)?),
        SqlType::Blob => SqlValue::Blob(row.try_get::<Vec<u8>, _>(idx)?),
    };
    Ok(value)
}

/// True when `err` is PostgreSQL's `undefined_table` (SQLSTATE `42P01`) — the
/// signal that nothing has ever been written for a table. The SQLite store
/// detects the same condition by matching `"no such table"` in the driver's
/// message; PostgreSQL exposes a stable SQLSTATE instead.
fn is_undefined_table(err: &sqlx::Error) -> bool {
    matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42P01"))
}

#[async_trait]
impl Store for PostgresStore {
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        // Progress table tracking the last processed block per event table.
        sqlx::query(&format!(
            "CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} \
             (table_name TEXT PRIMARY KEY, last_block BIGINT NOT NULL)"
        ))
        .execute(&mut *tx)
        .await?;

        // Create the event table on demand: an implicit block_number column
        // plus one column per event field, typed by the PostgreSQL mapping.
        let mut defs = vec![format!("{BLOCK_NUMBER_COLUMN} BIGINT NOT NULL")];
        for c in &schema.columns {
            defs.push(format!("{} {}", quote_ident(&c.name), pg_column_type(c.ty)));
        }
        let create = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            quote_ident(&schema.table),
            defs.join(", ")
        );
        sqlx::query(&create).execute(&mut *tx).await?;

        // Insert every row in the block with positional ($N) placeholders.
        let mut col_names = vec![BLOCK_NUMBER_COLUMN.to_string()];
        col_names.extend(schema.columns.iter().map(|c| quote_ident(&c.name)));
        let placeholders = (1..=col_names.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let insert = format!(
            "INSERT INTO {} ({}) VALUES ({})",
            quote_ident(&schema.table),
            col_names.join(", "),
            placeholders
        );

        for row in &rows {
            // Guard against shape mismatches before binding: a short argument
            // list would desync columns from values. Bail (rolling the
            // transaction back) instead, mirroring SqliteStore's guard
            // (postgres-store.PGSTORE.6).
            if row.0.len() != schema.columns.len() {
                anyhow::bail!(
                    "row has {} values but table {:?} has {} columns",
                    row.0.len(),
                    schema.table,
                    schema.columns.len()
                );
            }
            let mut args = PgArguments::default();
            bind_value(&mut args, &SqlValue::Integer(block as i64))?;
            for value in &row.0 {
                bind_value(&mut args, value)?;
            }
            sqlx::query_with(&insert, args).execute(&mut *tx).await?;
        }

        // Advance the last processed block in the same transaction. GREATEST
        // keeps the watermark monotonic so it never regresses
        // (postgres-store.DURABILITY.2).
        sqlx::query(&format!(
            "INSERT INTO {PROGRESS_TABLE} (table_name, last_block) VALUES ($1, $2) \
             ON CONFLICT (table_name) DO UPDATE \
             SET last_block = GREATEST({PROGRESS_TABLE}.last_block, excluded.last_block)"
        ))
        .bind(&schema.table)
        .bind(block as i64)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        let query = format!("SELECT last_block FROM {PROGRESS_TABLE} WHERE table_name = $1");
        let row = match sqlx::query(&query)
            .bind(table)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            // No progress table means nothing has ever been written.
            Err(e) if is_undefined_table(&e) => None,
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
            "SELECT {} FROM {} WHERE {BLOCK_NUMBER_COLUMN} <= $1 \
             ORDER BY {BLOCK_NUMBER_COLUMN} ASC, ctid ASC",
            select_cols,
            quote_ident(&schema.table)
        );

        let pg_rows = match sqlx::query(&sql)
            .bind(to as i64)
            .fetch_all(&self.pool)
            .await
        {
            Ok(rows) => rows,
            // A missing table means nothing has been stored yet.
            Err(e) if is_undefined_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut out = Vec::with_capacity(pg_rows.len());
        for r in &pg_rows {
            let mut values = Vec::with_capacity(schema.columns.len());
            for (idx, c) in schema.columns.iter().enumerate() {
                values.push(decode_value(r, idx, c.ty)?);
            }
            out.push(Row(values));
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema::{SqlType, TableSchema};

    // Reserved-name rejection (postgres-store.PGSTORE.7; NoReservedNames →
    // ReservedNameRejected) is backend-agnostic: `Record`/`Persisted` call
    // `TableSchema::ensure_no_reserved_names` on the user's schema BEFORE any
    // Store sees it (persisted.rs / record.rs). `PostgresStore` deliberately
    // does NOT re-check inside `write_block` — exactly like `SqliteStore` — both
    // because that would duplicate the upstream guard and because the schema
    // reaching the Store legitimately carries the reserved `_payload` column.
    // This test pins the shared guard `PostgresStore` relies on.
    #[test]
    fn shared_reserved_name_guard_rejects_reserved_identifiers() {
        // Reserved bookkeeping table name.
        assert!(
            TableSchema::new("_artemis_progress")
                .col("value", SqlType::Text)
                .ensure_no_reserved_names()
                .is_err()
        );
        // Reserved implicit column names.
        assert!(
            TableSchema::new("evt")
                .col("block_number", SqlType::Integer)
                .ensure_no_reserved_names()
                .is_err()
        );
        assert!(
            TableSchema::new("evt")
                .col("_payload", SqlType::Text)
                .ensure_no_reserved_names()
                .is_err()
        );
        // A clean user schema passes.
        assert!(
            TableSchema::new("evt")
                .col("value", SqlType::Text)
                .ensure_no_reserved_names()
                .is_ok()
        );
    }
}
