//! The generic SQL [`Store`]: one orchestration body over any sqlx
//! [`Database`], with a [`Dialect`] supplying the tokens that differ between
//! backends.
//!
//! The two concrete stores are thin type aliases over this one type
//! (`SqliteStore = SqlStore<Sqlite, SqliteDialect>`, and the PostgreSQL twin),
//! so `write` / `stored_position` / `replay` exist exactly once — a single
//! generic `impl<P: Position, DB, D> Store<P> for SqlStore<DB, D>` body serves
//! every position type [position-trait.MODULE.1]. The price is the sqlx generic
//! trait-bound wall on the [`Store`] impl below — paid once, concentrated here.
//! Per-backend connection tuning stays in the [`sqlite`](super::sqlite) /
//! [`postgres`](super::postgres) constructors; per-backend value binding and
//! cell decoding are the only behaviour that genuinely varies, and they ride
//! sqlx's own per-database types via the bounds.

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use async_trait::async_trait;
use sqlx::{ColumnIndex, Database, Decode, Encode, Executor, IntoArguments, Pool, Row as _, Type};

use super::dialect::Dialect;
use super::position::Position;
use super::query;
use super::schema::{Row, SqlType, SqlValue, TableSchema};
use super::store::Store;

/// A SQL-backed [`Store`] generic over the sqlx [`Database`] `DB` and its
/// [`Dialect`] `D`. Construct one through a backend's `connect` (see
/// [`SqliteStore`](super::SqliteStore)).
pub struct SqlStore<DB: Database, D: Dialect> {
    pool: Pool<DB>,
    dialect: D,
    /// Whether this store instance has already probed for (and, if needed,
    /// performed) the lazy integer→encoded-position progress migration. Gates the
    /// one-shot `SELECT position …` probe so only the first write per instance
    /// pays for it [position-trait.MIGRATE.1]. Set only after a successful commit,
    /// so a rolled-back write (which undoes any ADD COLUMN) leaves the next write
    /// to re-probe.
    migration_checked: AtomicBool,
}

impl<DB: Database, D: Dialect> SqlStore<DB, D> {
    /// Wrap an already-opened pool and its dialect. Backends call this from
    /// their own `connect`, which owns the per-backend pool tuning.
    pub(crate) fn new(pool: Pool<DB>, dialect: D) -> Self {
        Self {
            pool,
            dialect,
            migration_checked: AtomicBool::new(false),
        }
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
impl<P, DB, D> Store<P> for SqlStore<DB, D>
where
    P: Position,
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
    async fn write(&self, schema: &TableSchema, position: P, rows: Vec<Row>) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(&query::create_progress_table(&self.dialect))
            .execute(&mut *tx)
            .await?;
        sqlx::query(&query::create_event_table(schema, &self.dialect))
            .execute(&mut *tx)
            .await?;

        // Lazily migrate a pre-change two-column archive to the encoded-position
        // schema, once per store instance and inside this same write transaction
        // so the schema change and the rows it enables commit or roll back together
        // [position-trait.MIGRATE.1]. `CREATE TABLE IF NOT EXISTS` above is a no-op
        // for an existing two-column table, so the probe below still sees the old
        // shape. The probe runs under a SAVEPOINT: its undefined-column error is,
        // on PostgreSQL, an aborted-transaction signal that would poison the outer
        // write transaction, so rolling back to the savepoint clears it before the
        // ADD COLUMN + CAST backfill run in the same transaction.
        if !self.migration_checked.load(Ordering::Relaxed) {
            sqlx::query(query::MIGRATION_SAVEPOINT_BEGIN)
                .execute(&mut *tx)
                .await?;
            match sqlx::query(&query::probe_position_column())
                .fetch_optional(&mut *tx)
                .await
            {
                // The `position` column is present: nothing to migrate.
                Ok(_) => {
                    sqlx::query(query::MIGRATION_SAVEPOINT_RELEASE)
                        .execute(&mut *tx)
                        .await?;
                }
                // A pre-migration archive: undo the poisoned probe, then add the
                // column and convert every integer last_block into its encoded
                // BlockPosition via CAST(last_block AS TEXT) [position-trait.MIGRATE.1].
                Err(e) if self.dialect.is_undefined_column(&e) => {
                    sqlx::query(query::MIGRATION_SAVEPOINT_ROLLBACK)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(query::MIGRATION_SAVEPOINT_RELEASE)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(&query::add_position_column())
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query(&query::backfill_position_from_last_block())
                        .execute(&mut *tx)
                        .await?;
                }
                Err(e) => return Err(e.into()),
            }
        }

        // The sort key is the totally-ordered scalar bound into the implicit
        // `block_number` column — byte-identical to today's block number for
        // `BlockPosition` [position-trait.PARITY.1].
        let sort_key = position.sort_key();
        let insert = query::insert_statement(schema, &self.dialect);
        for row in &rows {
            query::check_row_shape(schema, row)?;
            let mut args = <DB::Arguments<'_>>::default();
            bind_value::<DB>(&mut args, &SqlValue::Integer(sort_key as i64))?;
            for value in &row.0 {
                bind_value::<DB>(&mut args, value)?;
            }
            sqlx::query_with(&insert, args).execute(&mut *tx).await?;
        }

        // Read the previous watermark under the row lock, then advance it in Rust
        // via `Position::advance` and upsert the result — all in this one
        // transaction, replacing the former SQL MAX/GREATEST [position-trait.ATOMIC.1].
        let select = query::locked_progress_select(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(schema.table.clone()))?;
        let prev_row = sqlx::query_with(&select, args)
            .fetch_optional(&mut *tx)
            .await?;
        // Reconstruct the previous position from the authoritative encoded
        // `position` column, guaranteed present by the migration above
        // [position-trait.MIGRATE.3]. A NULL cell cannot occur here (the migration
        // backfills every row and this upsert always writes it), so a present row
        // always yields an encoded value to decode.
        let prev = match prev_row {
            Some(r) => match r.get::<Option<String>, _>(0) {
                Some(encoded) => Some(P::decode(&encoded)?),
                None => None,
            },
            None => None,
        };
        let prev_key = prev.as_ref().map(Position::sort_key);
        let next = P::advance(prev, position);
        if let Some(prev_key) = prev_key {
            debug_assert!(
                next.sort_key() >= prev_key,
                "Position::advance must be monotone in sort key (advanced to {} from {prev_key})",
                next.sort_key()
            );
        }

        // Upsert both columns: the retained `last_block` sort key (the serving
        // layer keeps reading it) and the authoritative encoded `position`. For
        // `BlockPosition` the encoded text is the same decimal as `last_block`
        // [position-trait.MIGRATE.3].
        let upsert = query::watermark_upsert(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(schema.table.clone()))?;
        bind_value::<DB>(&mut args, &SqlValue::Integer(next.sort_key() as i64))?;
        bind_value::<DB>(&mut args, &SqlValue::Text(next.encode()))?;
        sqlx::query_with(&upsert, args).execute(&mut *tx).await?;

        tx.commit().await?;
        // Record the migration check only after a successful commit: a rolled-back
        // write undoes any ADD COLUMN, so the next write must re-probe.
        self.migration_checked.store(true, Ordering::Relaxed);
        Ok(())
    }

    async fn stored_position(&self, table: &str) -> Result<Option<P>> {
        // Primary read: the authoritative encoded `position` column.
        let sql = query::stored_position_query(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(table.to_string()))?;
        let encoded: Option<Option<String>> = match sqlx::query_with(&sql, args)
            .fetch_optional(&self.pool)
            .await
        {
            // Row present: extract the (possibly NULL) `position` cell.
            Ok(Some(r)) => Some(r.get::<Option<String>, _>(0)),
            // No row for this table: nothing stored.
            Ok(None) => return Ok(None),
            // Nothing has ever been written: the progress table does not exist.
            Err(e) if self.dialect.is_undefined_table(&e) => return Ok(None),
            // A pre-migration archive: the `position` column does not exist yet, so
            // fall back to decoding `last_block`'s decimal text below.
            Err(e) if self.dialect.is_undefined_column(&e) => None,
            Err(e) => return Err(e.into()),
        };

        match encoded {
            // The authoritative encoded resume point is present: decode it. A
            // malformed / wrong-typed value fails loudly here (MalformedStoredPosition
            // propagated verbatim from `Position::decode`), never a silent genesis
            // re-sync [position-trait.MIGRATE.2].
            Some(Some(text)) => Ok(Some(P::decode(&text)?)),
            // The column is absent (pre-migration archive) or NULL (an old binary
            // wrote after the migration): resume from `last_block`'s decimal text,
            // so an old block archive resumes at the same block before its first
            // write [position-trait.MIGRATE.2].
            Some(None) | None => self.resume_from_last_block(table).await,
        }
    }

    async fn replay(&self, schema: &TableSchema, up_to: P) -> Result<Vec<Row>> {
        let sql = query::replay_query(schema, &self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Integer(up_to.sort_key() as i64))?;
        let rows = match sqlx::query_with(&sql, args).fetch_all(&self.pool).await {
            Ok(rows) => rows,
            // A missing table means nothing has been stored yet.
            Err(e) if self.dialect.is_undefined_table(&e) => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };
        query::collect_rows(&rows, schema, |r, idx, ty| decode_value::<DB>(r, idx, ty))
    }
}

impl<DB, D> SqlStore<DB, D>
where
    DB: Database,
    D: Dialect,
    for<'c> &'c Pool<DB>: Executor<'c, Database = DB>,
    for<'q> DB::Arguments<'q>: Default + IntoArguments<'q, DB>,
    usize: ColumnIndex<DB::Row>,
    for<'q> i64: Encode<'q, DB>,
    for<'q> f64: Encode<'q, DB>,
    for<'q> String: Encode<'q, DB>,
    for<'q> Vec<u8>: Encode<'q, DB>,
    for<'q> Option<i64>: Encode<'q, DB>,
    for<'r> i64: Decode<'r, DB>,
    i64: Type<DB>,
    f64: Type<DB>,
    String: Type<DB>,
    Vec<u8>: Type<DB>,
    Option<i64>: Type<DB>,
{
    /// Read `last_block` for `table` and decode it as a [`Position`] from its
    /// decimal text — the read-side fallback used by [`stored_position`] when the
    /// encoded `position` column is absent (a pre-migration archive) or NULL. Lets
    /// an old block archive resume at the same block before its first write
    /// [position-trait.MIGRATE.2].
    async fn resume_from_last_block<P: Position>(&self, table: &str) -> Result<Option<P>> {
        let sql = query::last_block_query(&self.dialect);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &SqlValue::Text(table.to_string()))?;
        let row = match sqlx::query_with(&sql, args)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            Err(e) if self.dialect.is_undefined_table(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        match row {
            Some(r) => Ok(Some(P::decode(&r.get::<i64, _>(0).to_string())?)),
            None => Ok(None),
        }
    }
}
