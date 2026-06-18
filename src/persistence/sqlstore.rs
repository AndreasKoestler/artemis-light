//! The generic SQL [`Store`]: one orchestration body over any sqlx
//! [`Database`], with a [`Dialect`] supplying the tokens that differ between
//! backends.
//!
//! The two concrete stores are thin type aliases over this one type
//! (`SqliteStore = SqlStore<Sqlite, SqliteDialect>`, and the PostgreSQL twin),
//! so `write_block` / `last_block` / `replay` exist exactly once. The price is
//! the sqlx generic trait-bound wall on the [`Store`] impl below — paid once,
//! concentrated here. Per-backend connection tuning stays in the
//! [`sqlite`](super::sqlite) / [`postgres`](super::postgres) constructors;
//! per-backend value binding and cell decoding are the only behaviour that
//! genuinely varies, and they ride sqlx's own per-database types via the bounds.

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Pool, Row as _, Type};

use super::dialect::Dialect;
use super::query;
use super::schema::{Row, SqlType, SqlValue, TableSchema};
use super::store::Store;

/// A SQL-backed [`Store`] generic over the sqlx [`Database`] `DB` and its
/// [`Dialect`] `D`. Construct one through a backend's `connect` (see
/// [`SqliteStore`](super::SqliteStore)).
pub struct SqlStore<DB: Database, D: Dialect> {
    pool: Pool<DB>,
    dialect: D,
}

impl<DB: Database, D: Dialect> SqlStore<DB, D> {
    /// Wrap an already-opened pool and its dialect. Backends call this from
    /// their own `connect`, which owns the per-backend pool tuning.
    pub(crate) fn new(pool: Pool<DB>, dialect: D) -> Self {
        Self { pool, dialect }
    }
}

/// Bind one [`SqlValue`] onto a backend's argument list. The per-database
/// argument type is the only thing that varies; the `SqlValue` match is shared.
fn bind_value<'q, DB>(args: &mut DB::Arguments<'q>, value: &SqlValue) -> Result<()>
where
    DB: Database,
    i64: Encode<'q, DB> + Type<DB>,
    f64: Encode<'q, DB> + Type<DB>,
    String: Encode<'q, DB> + Type<DB>,
    Vec<u8>: Encode<'q, DB> + Type<DB>,
    Option<i64>: Encode<'q, DB> + Type<DB>,
{
    use sqlx::Arguments as _;
    match value {
        SqlValue::Integer(i) => args.add(*i),
        SqlValue::Real(r) => args.add(*r),
        SqlValue::Text(s) => args.add(s.clone()),
        SqlValue::Blob(b) => args.add(b.clone()),
        SqlValue::Null => args.add(None::<i64>),
    }
    .map_err(|e| anyhow::anyhow!("failed to bind value: {e}"))
}

/// Decode column `idx` of a backend row into a [`SqlValue`] per its declared
/// type. `Numeric` decodes as text (same arm as `Text`) so replay round-trips
/// to logically identical rows across backends (postgres-store.PARITY.1).
fn decode_value<DB>(row: &DB::Row, idx: usize, ty: SqlType) -> Result<SqlValue>
where
    DB: Database,
    usize: ColumnIndex<DB::Row>,
    for<'r> i64: Decode<'r, DB>,
    for<'r> f64: Decode<'r, DB>,
    for<'r> String: Decode<'r, DB>,
    for<'r> Vec<u8>: Decode<'r, DB>,
    i64: Type<DB>,
    f64: Type<DB>,
    String: Type<DB>,
    Vec<u8>: Type<DB>,
{
    let value = match ty {
        SqlType::Integer => SqlValue::Integer(row.try_get::<i64, _>(idx)?),
        SqlType::Real => SqlValue::Real(row.try_get::<f64, _>(idx)?),
        SqlType::Text | SqlType::Numeric => SqlValue::Text(row.try_get::<String, _>(idx)?),
        SqlType::Blob => SqlValue::Blob(row.try_get::<Vec<u8>, _>(idx)?),
    };
    Ok(value)
}

#[async_trait]
impl<DB, D> Store for SqlStore<DB, D>
where
    DB: Database,
    D: Dialect,
    // Both the pool (reads) and an in-flight transaction (writes) must be usable
    // as sqlx executors for this database.
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    // Build a fresh argument list, then hand it to `query_with`.
    for<'q> DB::Arguments<'q>: Default + IntoArguments<'q, DB>,
    usize: ColumnIndex<DB::Row>,
    // Every `SqlValue` arm must encode (writes) and decode (replay) for `DB`.
    for<'q> i64: Encode<'q, DB>,
    for<'q> f64: Encode<'q, DB>,
    for<'q> String: Encode<'q, DB>,
    for<'q> Vec<u8>: Encode<'q, DB>,
    for<'q> Option<i64>: Encode<'q, DB>,
    for<'r> i64: Decode<'r, DB>,
    for<'r> f64: Decode<'r, DB>,
    for<'r> String: Decode<'r, DB>,
    for<'r> Vec<u8>: Decode<'r, DB>,
    i64: Type<DB>,
    f64: Type<DB>,
    String: Type<DB>,
    Vec<u8>: Type<DB>,
    Option<i64>: Type<DB>,
{
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(&query::create_progress_table(&self.dialect))
            .execute(&mut *tx)
            .await?;
        sqlx::query(&query::create_event_table(schema, &self.dialect))
            .execute(&mut *tx)
            .await?;

        let insert = query::insert_statement(schema, &self.dialect);
        for row in &rows {
            query::check_row_shape(schema, row)?;
            let mut args = <DB::Arguments<'_>>::default();
            bind_value::<DB>(&mut args, &SqlValue::Integer(block as i64))?;
            for value in &row.0 {
                bind_value::<DB>(&mut args, value)?;
            }
            sqlx::query_with(&insert, args).execute(&mut *tx).await?;
        }

        // Advance the last processed block in the same transaction; the dialect's
        // monotonic max keeps the watermark from regressing.
        let upsert = query::watermark_upsert(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(schema.table.clone()))?;
        bind_value::<DB>(&mut args, &SqlValue::Integer(block as i64))?;
        sqlx::query_with(&upsert, args).execute(&mut *tx).await?;

        tx.commit().await?;
        Ok(())
    }

    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        let sql = query::last_block_query(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(table.to_string()))?;
        let row = match sqlx::query_with(&sql, args)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            // Nothing has ever been written: the progress table does not exist.
            Err(e) if self.dialect.is_undefined_table(&e) => None,
            Err(e) => return Err(e.into()),
        };
        Ok(row.map(|r| r.get::<i64, _>(0) as u64))
    }

    async fn replay(&self, schema: &TableSchema, to: u64) -> Result<Vec<Row>> {
        let sql = query::replay_query(schema, &self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Integer(to as i64))?;
        let rows = match sqlx::query_with(&sql, args).fetch_all(&self.pool).await {
            Ok(rows) => rows,
            // A missing table means nothing has been stored yet.
            Err(e) if self.dialect.is_undefined_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        query::collect_rows(&rows, schema, |r, idx, ty| decode_value::<DB>(r, idx, ty))
    }
}
