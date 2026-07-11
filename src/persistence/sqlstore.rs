//! The generic SQL [`Store`]: one orchestration body over any sqlx
//! [`Database`], with a [`Dialect`] supplying the tokens that differ between
//! backends.
//!
//! The two concrete stores are thin type aliases over this one type
//! (`SqliteStore = SqlStore<Sqlite, SqliteDialect>`, and the PostgreSQL twin),
//! so `write` / `stored_position` / `replay` exist exactly once — a single
//! generic `impl<P: Position, DB, D> Store<P> for SqlStore<DB, D>` body serves
//! every position type. The price is the sqlx generic
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
    /// pays for it. Set only after a successful commit,
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

    /// Wrap a DDL/migration failure the dialect classifies as a benign
    /// duplicate-object race in the [`DdlRace`] marker `write` retries on;
    /// any other failure propagates unchanged.
    fn classify_ddl(&self, e: sqlx::Error) -> anyhow::Error {
        if self.dialect.is_duplicate_object(&e) {
            anyhow::Error::new(DdlRace(e))
        } else {
            e.into()
        }
    }
}

/// Marker wrapping a backend error from the DDL/migration path that the
/// [`Dialect`] classified as a benign duplicate-object race: two writers on a
/// shared multi-connection pool issued the same `CREATE TABLE IF NOT EXISTS`
/// or lazy `ADD COLUMN`, and the loser's statement failed even though the
/// object now exists. `write` retries such a failure once instead of
/// propagating it into a permanently unhealthy writer.
#[derive(Debug)]
struct DdlRace(sqlx::Error);

impl std::fmt::Display for DdlRace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "lost a concurrent DDL race (object already exists)")
    }
}

impl std::error::Error for DdlRace {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}

/// Convert a [`Position::sort_key`] to the signed 64-bit value the
/// `block_number` / `last_block` columns store. Sort keys must stay at or
/// below `i64::MAX`; beyond it this fails descriptively rather than letting
/// `as i64` wrap the key negative and silently corrupt the archive's order.
fn sort_key_to_i64(key: u64) -> Result<i64> {
    i64::try_from(key).map_err(|_| {
        anyhow::anyhow!(
            "position sort key {key} exceeds i64::MAX ({}); \
             sort keys are stored in signed 64-bit SQL columns",
            i64::MAX
        )
    })
}

/// Conservative cap on bind parameters per statement, shared across backends
/// so the chunk shape never depends on the dialect (SQLite's historical
/// default limit is 999; PostgreSQL's protocol limit is 65535).
const BIND_PARAM_CAP: usize = 999;

/// How many rows fit one multi-row `INSERT` chunk when each row binds
/// `params_per_row` parameters — at least one row, even for a table wider
/// than the cap itself.
fn rows_per_chunk(params_per_row: usize) -> usize {
    (BIND_PARAM_CAP / params_per_row.max(1)).max(1)
}

/// Bind one [`SqlValue`] onto a backend's argument list. The per-database
/// argument type is the only thing that varies; the `SqlValue` match is
/// shared. Text and blob cells bind as borrows, so the write hot path clones
/// no cell payloads.
fn bind_value<'q, DB>(args: &mut DB::Arguments<'q>, value: &'q SqlValue) -> Result<()>
where
    DB: Database,
    i64: Encode<'q, DB> + Type<DB>,
    f64: Encode<'q, DB> + Type<DB>,
    &'q str: Encode<'q, DB>,
    &'q [u8]: Encode<'q, DB>,
    str: Type<DB>,
    [u8]: Type<DB>,
    Option<i64>: Encode<'q, DB> + Type<DB>,
{
    use sqlx::Arguments as _;
    match value {
        SqlValue::Integer(i) => args.add(*i),
        SqlValue::Real(r) => args.add(*r),
        SqlValue::Text(s) => args.add(s.as_str()),
        SqlValue::Blob(b) => args.add(b.as_slice()),
        SqlValue::Null => args.add(None::<i64>),
    }
    .map_err(|e| anyhow::anyhow!("failed to bind value: {e}"))
}

/// Decode column `idx` of a backend row into a [`SqlValue`] per its declared
/// type. `Numeric` decodes as text (same arm as `Text`) so replay round-trips
/// to logically identical rows across backends.
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
    // Every `SqlValue` arm must encode (writes; text/blob bind as borrows)
    // and decode (replay) for `DB`.
    for<'q> i64: Encode<'q, DB>,
    for<'q> f64: Encode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB>,
    for<'q> Option<i64>: Encode<'q, DB>,
    for<'r> i64: Decode<'r, DB>,
    for<'r> f64: Decode<'r, DB>,
    for<'r> String: Decode<'r, DB>,
    for<'r> Vec<u8>: Decode<'r, DB>,
    i64: Type<DB>,
    f64: Type<DB>,
    str: Type<DB>,
    [u8]: Type<DB>,
    String: Type<DB>,
    Vec<u8>: Type<DB>,
    Option<i64>: Type<DB>,
{
    async fn write(&self, schema: &TableSchema, position: P, rows: Vec<Row>) -> Result<()> {
        match self.write_once(schema, position.clone(), &rows).await {
            // The losing side of a concurrent DDL race on a shared
            // multi-connection pool: the table/column already exists, so one
            // retry runs against clean no-op DDL instead of leaving the
            // gap-free writer permanently unhealthy.
            Err(e) if e.downcast_ref::<DdlRace>().is_some() => {
                self.write_once(schema, position, &rows).await
            }
            result => result,
        }
    }

    async fn stored_position(&self, table: &str) -> Result<Option<P>> {
        // Primary read: the authoritative encoded `position` column.
        let sql = query::stored_position_query(&self.dialect);
        let table_cell = SqlValue::Text(table.to_string());
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &table_cell)?;
        let encoded: Option<Option<String>> = match sqlx::query_with(&sql, args)
            .fetch_optional(&self.pool)
            .await
        {
            // Row present: extract the (possibly NULL) `position` cell. A
            // wrong-typed cell in a foreign/corrupted progress table is a
            // propagated decode error, never a panic inside library code.
            Ok(Some(r)) => Some(r.try_get::<Option<String>, _>(0)?),
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
            // malformed / wrong-typed value fails loudly here (the `Position::decode`
            // error propagated verbatim), never a silent genesis re-sync.
            Some(Some(text)) => Ok(Some(P::decode(&text)?)),
            // The column is absent (pre-migration archive) or NULL (an old binary
            // wrote after the migration): resume from `last_block`'s decimal text,
            // so an old block archive resumes at the same block before its first
            // write.
            Some(None) | None => self.resume_from_last_block(table).await,
        }
    }

    async fn replay(&self, schema: &TableSchema, up_to: P) -> Result<Vec<Row>> {
        let sql = query::replay_query(schema, &self.dialect);
        let up_to_cell = SqlValue::Integer(sort_key_to_i64(up_to.sort_key())?);
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &up_to_cell)?;
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
    for<'c> &'c mut DB::Connection: Executor<'c, Database = DB>,
    for<'q> DB::Arguments<'q>: Default + IntoArguments<'q, DB>,
    usize: ColumnIndex<DB::Row>,
    for<'q> i64: Encode<'q, DB>,
    for<'q> f64: Encode<'q, DB>,
    for<'q> &'q str: Encode<'q, DB>,
    for<'q> &'q [u8]: Encode<'q, DB>,
    for<'q> Option<i64>: Encode<'q, DB>,
    for<'r> i64: Decode<'r, DB>,
    for<'r> String: Decode<'r, DB>,
    i64: Type<DB>,
    f64: Type<DB>,
    str: Type<DB>,
    [u8]: Type<DB>,
    String: Type<DB>,
    Option<i64>: Type<DB>,
{
    /// One `write` attempt: the full DDL → lazy migration → row inserts →
    /// watermark advance transaction. Split from [`Store::write`] so a benign
    /// concurrent-DDL failure (a [`DdlRace`]) can be retried once against the
    /// same `rows`.
    async fn write_once<P: Position>(
        &self,
        schema: &TableSchema,
        position: P,
        rows: &[Row],
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        self.create_tables(&mut tx, schema).await?;
        self.migrate_if_needed(&mut tx).await?;

        // The sort key is the totally-ordered scalar bound into the implicit
        // `block_number` column — byte-identical to today's block number for
        // `BlockPosition`.
        let sort_key = SqlValue::Integer(sort_key_to_i64(position.sort_key())?);
        self.insert_rows(&mut tx, schema, &sort_key, rows).await?;
        self.advance_watermark(&mut tx, schema, position).await?;

        tx.commit().await?;
        // Record the migration check only after a successful commit: a rolled-back
        // write undoes any ADD COLUMN, so the next write must re-probe.
        self.migration_checked.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Create the progress and event tables if absent. A DDL failure the dialect
    /// classifies as a benign duplicate-object race is wrapped in [`DdlRace`] so
    /// `write` can retry it once.
    async fn create_tables(&self, conn: &mut DB::Connection, schema: &TableSchema) -> Result<()> {
        sqlx::query(&query::create_progress_table(&self.dialect))
            .execute(&mut *conn)
            .await
            .map_err(|e| self.classify_ddl(e))?;
        sqlx::query(&query::create_event_table(schema, &self.dialect))
            .execute(&mut *conn)
            .await
            .map_err(|e| self.classify_ddl(e))?;
        Ok(())
    }

    /// Lazily migrate a pre-change two-column archive to the encoded-position
    /// schema, once per store instance and inside the caller's write transaction
    /// so the schema change and the rows it enables commit or roll back together.
    /// `CREATE TABLE IF NOT EXISTS` (run by [`create_tables`](Self::create_tables))
    /// is a no-op for an existing two-column table, so the probe below still sees
    /// the old shape. The probe runs under a SAVEPOINT: its undefined-column error
    /// is, on PostgreSQL, an aborted-transaction signal that would poison the
    /// outer write transaction, so rolling back to the savepoint clears it before
    /// the ADD COLUMN + CAST backfill run in the same transaction.
    async fn migrate_if_needed(&self, conn: &mut DB::Connection) -> Result<()> {
        if self.migration_checked.load(Ordering::Relaxed) {
            return Ok(());
        }
        sqlx::query(query::MIGRATION_SAVEPOINT_BEGIN)
            .execute(&mut *conn)
            .await?;
        match sqlx::query(&query::probe_position_column())
            .fetch_optional(&mut *conn)
            .await
        {
            // The `position` column is present: nothing to migrate.
            Ok(_) => {
                sqlx::query(query::MIGRATION_SAVEPOINT_RELEASE)
                    .execute(&mut *conn)
                    .await?;
            }
            // A pre-migration archive: undo the poisoned probe, then add the
            // column and convert every integer last_block into its encoded
            // BlockPosition via CAST(last_block AS TEXT).
            Err(e) if self.dialect.is_undefined_column(&e) => {
                sqlx::query(query::MIGRATION_SAVEPOINT_ROLLBACK)
                    .execute(&mut *conn)
                    .await?;
                sqlx::query(query::MIGRATION_SAVEPOINT_RELEASE)
                    .execute(&mut *conn)
                    .await?;
                sqlx::query(&query::add_position_column())
                    .execute(&mut *conn)
                    .await
                    .map_err(|e| self.classify_ddl(e))?;
                sqlx::query(&query::backfill_position_from_last_block())
                    .execute(&mut *conn)
                    .await?;
            }
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    /// Insert `rows` as multi-row inserts — one round-trip per chunk instead of
    /// per row — chunked so the bind parameters stay under the shared cap. The
    /// full-chunk statement is built once and reused; only a trailing partial
    /// chunk needs its own. Each row binds `sort_key` (the implicit
    /// `block_number` column) ahead of its own cells.
    async fn insert_rows(
        &self,
        conn: &mut DB::Connection,
        schema: &TableSchema,
        sort_key: &SqlValue,
        rows: &[Row],
    ) -> Result<()> {
        let params_per_row = 1 + schema.columns.len();
        let chunk_rows = rows_per_chunk(params_per_row);
        let mut full_chunk_insert: Option<String> = None;
        for chunk in rows.chunks(chunk_rows) {
            let partial_chunk_insert;
            let insert = if chunk.len() == chunk_rows {
                full_chunk_insert
                    .get_or_insert_with(|| {
                        query::insert_statement(schema, &self.dialect, chunk_rows)
                    })
                    .as_str()
            } else {
                partial_chunk_insert = query::insert_statement(schema, &self.dialect, chunk.len());
                &partial_chunk_insert
            };
            let mut args = <DB::Arguments<'_>>::default();
            for row in chunk {
                query::check_row_shape(schema, row)?;
                bind_value::<DB>(&mut args, sort_key)?;
                for value in &row.0 {
                    bind_value::<DB>(&mut args, value)?;
                }
            }
            sqlx::query_with(insert, args).execute(&mut *conn).await?;
        }
        Ok(())
    }

    /// Read the previous watermark under the row lock, advance it in Rust via
    /// [`Position::advance`], and upsert the result: the retained `last_block`
    /// sort key (the serving layer keeps reading it) and the authoritative
    /// encoded `position`. For `BlockPosition` the encoded text is the same
    /// decimal as `last_block`.
    async fn advance_watermark<P: Position>(
        &self,
        conn: &mut DB::Connection,
        schema: &TableSchema,
        position: P,
    ) -> Result<()> {
        let select = query::locked_progress_select(&self.dialect);
        let table_cell = SqlValue::Text(schema.table.clone());
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &table_cell)?;
        let prev_row = sqlx::query_with(&select, args)
            .fetch_optional(&mut *conn)
            .await?;
        // Reconstruct the previous position from the authoritative encoded
        // `position` column, guaranteed present by the migration above.
        // A NULL cell cannot occur here (the migration
        // backfills every row and this upsert always writes it), so a present row
        // always yields an encoded value to decode.
        let prev = match prev_row {
            Some(r) => match r.try_get::<Option<String>, _>(0)? {
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

        // Upsert both columns.
        let upsert = query::watermark_upsert(&self.dialect);
        let next_key_cell = SqlValue::Integer(sort_key_to_i64(next.sort_key())?);
        let next_position_cell = SqlValue::Text(next.encode());
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &table_cell)?;
        bind_value::<DB>(&mut args, &next_key_cell)?;
        bind_value::<DB>(&mut args, &next_position_cell)?;
        sqlx::query_with(&upsert, args).execute(&mut *conn).await?;
        Ok(())
    }

    /// Read `last_block` for `table` and decode it as a [`Position`] from its
    /// decimal text — the read-side fallback used by [`stored_position`] when the
    /// encoded `position` column is absent (a pre-migration archive) or NULL. Lets
    /// an old block archive resume at the same block before its first write.
    async fn resume_from_last_block<P: Position>(&self, table: &str) -> Result<Option<P>> {
        let sql = query::last_block_query(&self.dialect);
        let table_cell = SqlValue::Text(table.to_string());
        let mut args = <DB::Arguments<'_>>::default();
        bind_value::<DB>(&mut args, &table_cell)?;
        let row = match sqlx::query_with(&sql, args)
            .fetch_optional(&self.pool)
            .await
        {
            Ok(row) => row,
            Err(e) if self.dialect.is_undefined_table(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        // Decode defensively: this table may predate this crate or belong to a
        // foreign writer, so a wrong-typed or NULL cell is a propagated error
        // (never a panic, never a silent block-0 resume).
        match row {
            Some(r) => match r.try_get::<Option<i64>, _>(0)? {
                Some(last_block) => Ok(Some(P::decode(&last_block.to_string())?)),
                None => Err(anyhow::anyhow!(
                    "progress row for table {table:?} has a NULL last_block and no \
                     encoded position; the progress table is corrupt or foreign"
                )),
            },
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use sqlx::Sqlite;

    use super::*;
    use crate::persistence::dialect::SqliteDialect;
    use crate::persistence::position::BlockPosition;

    /// The [`DdlRace`] marker renders a stable, source-preserving message: the
    /// retry path matches on the type, but the logs still see the underlying
    /// backend error through `source`.
    #[test]
    fn ddl_race_displays_a_stable_message_and_exposes_its_source() {
        let race = DdlRace(sqlx::Error::PoolClosed);
        assert_eq!(
            race.to_string(),
            "lost a concurrent DDL race (object already exists)"
        );
        assert!(std::error::Error::source(&race).is_some());
    }

    /// A SQLite-backed dialect whose first `column_type` is deliberately
    /// malformed — so the first write attempt fails at the DDL, like a writer
    /// losing a concurrent CREATE TABLE / ADD COLUMN race — and whose
    /// `is_duplicate_object` verdict is configurable.
    struct RacyDialect {
        poison_first_ddl: AtomicBool,
        benign: bool,
    }

    impl RacyDialect {
        fn new(benign: bool) -> Self {
            Self {
                poison_first_ddl: AtomicBool::new(true),
                benign,
            }
        }
    }

    impl Dialect for RacyDialect {
        fn placeholder(&self, n: usize) -> String {
            SqliteDialect.placeholder(n)
        }

        fn tiebreak(&self) -> &'static str {
            SqliteDialect.tiebreak()
        }

        fn column_type(&self, ty: super::super::schema::SqlType) -> &'static str {
            if self.poison_first_ddl.swap(false, Ordering::SeqCst) {
                // Malformed keyword: the first CREATE TABLE fails outright.
                "INTEGER,"
            } else {
                SqliteDialect.column_type(ty)
            }
        }

        fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
            SqliteDialect.is_undefined_table(err)
        }

        fn is_undefined_column(&self, err: &sqlx::Error) -> bool {
            SqliteDialect.is_undefined_column(err)
        }

        fn is_duplicate_object(&self, _err: &sqlx::Error) -> bool {
            self.benign
        }
    }

    async fn memory_store<D: Dialect>(dialect: D) -> SqlStore<Sqlite, D> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        SqlStore::new(pool, dialect)
    }

    fn schema() -> TableSchema {
        TableSchema::new("t").col("v", SqlType::Text)
    }

    // The chunk size keeps `rows * params_per_row` under the shared bind cap,
    // with a one-row floor for tables wider than the cap itself.
    #[test]
    fn rows_per_chunk_stays_under_the_bind_parameter_cap() {
        assert_eq!(rows_per_chunk(2), 499);
        assert_eq!(rows_per_chunk(999), 1);
        // Wider than the cap: still one row per statement, never zero.
        assert_eq!(rows_per_chunk(5000), 1);
    }

    // A first write that loses a benign DDL race (the dialect classifies the
    // failure as duplicate-object) must retry once and succeed instead of
    // going permanently unhealthy.
    #[tokio::test]
    async fn write_retries_once_after_a_benign_ddl_race() {
        let store = memory_store(RacyDialect::new(true)).await;
        let rows = vec![Row(vec![SqlValue::Text("a".into())])];
        store
            .write(&schema(), BlockPosition(1), rows.clone())
            .await
            .unwrap();
        assert_eq!(
            store.replay(&schema(), BlockPosition(10)).await.unwrap(),
            rows
        );
        assert_eq!(
            Store::<BlockPosition>::stored_position(&store, "t")
                .await
                .unwrap(),
            Some(BlockPosition(1))
        );
    }

    // Sort keys are stored in signed 64-bit columns, so a key above
    // i64::MAX must fail loudly and descriptively — `as i64` would silently
    // wrap it negative, corrupting the archive's ordering.
    #[tokio::test]
    async fn write_and_replay_reject_a_sort_key_beyond_i64_max() {
        let store = memory_store(SqliteDialect).await;
        let rows = vec![Row(vec![SqlValue::Text("a".into())])];

        let err = store
            .write(&schema(), BlockPosition(u64::MAX), rows)
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("sort key"), "{msg}");
        assert!(msg.contains(&u64::MAX.to_string()), "{msg}");

        let err = store
            .replay(&schema(), BlockPosition(u64::MAX))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("sort key"), "{err:#}");
    }

    // The same DDL failure without the benign classification propagates: only
    // a dialect-confirmed duplicate-object race is retried.
    #[tokio::test]
    async fn write_propagates_a_ddl_failure_that_is_not_a_benign_race() {
        let store = memory_store(RacyDialect::new(false)).await;
        let result = store
            .write(
                &schema(),
                BlockPosition(1),
                vec![Row(vec![SqlValue::Text("a".into())])],
            )
            .await;
        assert!(result.is_err());
    }
}
