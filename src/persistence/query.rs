//! The pure SQL-text composition layer: dialect-parameterised query *strings*
//! shared by the generic [`SqlStore`](super::SqlStore) and the read serving
//! backends. These are free functions of `(schema, &dyn Dialect)` with no pool,
//! no `async`, and no sqlx trait bounds — so the generated SQL can be unit-tested
//! directly (see this module's tests) without standing up a database or the
//! generic store's bound wall.
//!
//! Every query whose shape is identical across backends lives here; the parts
//! that genuinely differ are supplied by a [`Dialect`]: the placeholder syntax,
//! the intra-block tie-breaker, the column-type keywords, and the progress
//! row-lock suffix. The two backends therefore cannot drift apart on the parts
//! they share. Watermark monotonicity lives in `Position::advance` in Rust,
//! applied inside the write transaction, so the upsert stores the
//! already-advanced watermark verbatim — no SQL `MAX`/`GREATEST`. The progress
//! table keeps the retained `last_block` sort key (the serving layer reads it)
//! alongside the authoritative encoded `position` column — `Position::encode`
//! output — which a pre-change archive gains through the lazy
//! [`add_position_column`] + [`backfill_position_from_last_block`] migration.
//! Per-backend value binding and cell decoding are *not* here — they ride
//! sqlx's per-database types and live in [`SqlStore`](super::SqlStore).

use anyhow::Result;

use super::dialect::Dialect;
use super::schema::{
    BLOCK_NUMBER_COLUMN, PROGRESS_TABLE, Row, SqlType, SqlValue, TableSchema, quote_ident,
};

/// The SQL name for a savepoint that brackets the one-shot migration probe.
///
/// A failed `SELECT position …` probe on a pre-migration archive is the
/// backend's undefined-column signal — but on PostgreSQL that error also *aborts*
/// the surrounding transaction, so every later statement in it would be rejected.
/// Running the probe inside this savepoint lets [`SqlStore`](super::SqlStore) roll
/// back to it (clearing the aborted state) and still run the ADD COLUMN + backfill
/// in the same write transaction. SQLite tolerates the failed probe directly but
/// is bracketed identically.
pub(super) const MIGRATION_SAVEPOINT_BEGIN: &str = "SAVEPOINT artemis_position_probe";
/// Discard the migration probe savepoint (see [`MIGRATION_SAVEPOINT_BEGIN`]).
pub(super) const MIGRATION_SAVEPOINT_RELEASE: &str = "RELEASE SAVEPOINT artemis_position_probe";
/// Undo a failed migration probe (see [`MIGRATION_SAVEPOINT_BEGIN`]), returning
/// the outer write transaction to a usable state before the ADD COLUMN + backfill.
pub(super) const MIGRATION_SAVEPOINT_ROLLBACK: &str =
    "ROLLBACK TO SAVEPOINT artemis_position_probe";

/// The column names an insert targets: the implicit `block_number` column
/// followed by one quoted column per event field, in schema order. Both stores
/// bind values in this same order.
pub(super) fn insert_column_names(schema: &TableSchema) -> Vec<String> {
    let mut col_names = vec![BLOCK_NUMBER_COLUMN.to_string()];
    col_names.extend(schema.columns.iter().map(|c| quote_ident(&c.name)));
    col_names
}

/// The comma-joined, quoted column list a replay `SELECT` projects, in schema
/// order — so decoded cells line up with `schema.columns` positionally.
fn select_column_list(schema: &TableSchema) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote_ident(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// `CREATE TABLE IF NOT EXISTS` for the bookkeeping progress table. `last_block`
/// takes the dialect's integer type (INTEGER / BIGINT) and is the retained sort
/// key the serving layer reads; `position` is the authoritative encoded resume
/// point (`Position::encode` output). A freshly created table declares `position`
/// `NOT NULL`; an old two-column archive gains it as a nullable column through the
/// lazy [`add_position_column`] migration (SQLite cannot `ADD COLUMN` a `NOT NULL`
/// column without a default), then [`backfill_position_from_last_block`] fills it
/// — both shapes read identically.
pub(super) fn create_progress_table(dialect: &dyn Dialect) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} \
         (table_name TEXT PRIMARY KEY, last_block {} NOT NULL, position TEXT NOT NULL)",
        dialect.column_type(SqlType::Integer)
    )
}

/// The lazy migration's first statement: add the encoded `position` column to a
/// pre-existing two-column progress table. Nullable (no `NOT NULL`) so SQLite
/// accepts it without a default; [`backfill_position_from_last_block`] fills it in
/// the same transaction. Dialect-independent: identical SQL on both backends.
pub(super) fn add_position_column() -> String {
    format!("ALTER TABLE {PROGRESS_TABLE} ADD COLUMN position TEXT")
}

/// The lazy migration's second statement: convert every pre-existing integer
/// `last_block` into its encoded `BlockPosition` — decimal text — via a pure-SQL
/// `CAST`, filling only the rows [`add_position_column`] left NULL. Idempotent
/// (the `WHERE position IS NULL` guard skips already-encoded rows) and
/// dialect-independent.
pub(super) fn backfill_position_from_last_block() -> String {
    format!(
        "UPDATE {PROGRESS_TABLE} SET position = CAST(last_block AS TEXT) WHERE position IS NULL"
    )
}

/// The one-shot migration probe: does the encoded `position` column exist? On a
/// pre-migration archive it returns the backend's undefined-column error
/// (classified by [`Dialect::is_undefined_column`]), driving the lazy ADD COLUMN +
/// backfill. `LIMIT 1` so it touches at most one row.
pub(super) fn probe_position_column() -> String {
    format!("SELECT position FROM {PROGRESS_TABLE} LIMIT 1")
}

/// `CREATE TABLE IF NOT EXISTS` for an event table: an implicit `block_number`
/// column plus one column per event field, each typed by the dialect.
pub(super) fn create_event_table(schema: &TableSchema, dialect: &dyn Dialect) -> String {
    let mut defs = vec![format!(
        "{BLOCK_NUMBER_COLUMN} {} NOT NULL",
        dialect.column_type(SqlType::Integer)
    )];
    for c in &schema.columns {
        defs.push(format!(
            "{} {}",
            quote_ident(&c.name),
            dialect.column_type(c.ty)
        ));
    }
    format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        quote_ident(&schema.table),
        defs.join(", ")
    )
}

/// The multi-row `INSERT`: one placeholder group per row (`rows` ≥ 1), each
/// with one dialect placeholder per bound column in [`insert_column_names`]
/// order, numbered continuously across rows for positional dialects. The
/// caller chunks its rows so the total placeholder count stays under the
/// backends' bind-parameter caps.
pub(super) fn insert_statement(schema: &TableSchema, dialect: &dyn Dialect, rows: usize) -> String {
    let col_names = insert_column_names(schema);
    let groups = (0..rows)
        .map(|row| {
            let placeholders = (1..=col_names.len())
                .map(|i| dialect.placeholder(row * col_names.len() + i))
                .collect::<Vec<_>>()
                .join(", ");
            format!("({placeholders})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES {}",
        quote_ident(&schema.table),
        col_names.join(", "),
        groups
    )
}

/// The watermark upsert: store both the retained `last_block` sort key (which the
/// serving layer keeps reading) and the authoritative encoded `position` for a
/// table, in the same transaction as its rows. Placeholders bind
/// `(table_name, last_block, position)`; both bound values are the already
/// monotonically-advanced watermark computed by `Position::advance` in Rust, so
/// the conflict clause writes them verbatim (`excluded.*`) with no SQL
/// `MAX`/`GREATEST` — a generic `Position`'s encoded text is not SQL-comparable,
/// so the database cannot enforce monotonicity itself.
///
/// The verbatim write is only safe under the one-writer-per-table contract:
/// [`locked_progress_select`]'s row lock serialises the read-advance-upsert
/// once a progress row exists, but it cannot lock a row that has not been
/// inserted yet, so two writers racing a table's *first* write can interleave
/// and regress the watermark. Concurrent writers on one table are a caller
/// violation this statement does not defend against.
pub(super) fn watermark_upsert(dialect: &dyn Dialect) -> String {
    format!(
        "INSERT INTO {PROGRESS_TABLE} (table_name, last_block, position) VALUES ({}, {}, {}) \
         ON CONFLICT (table_name) DO UPDATE SET \
         last_block = excluded.last_block, position = excluded.position",
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.placeholder(3),
    )
}

/// The in-transaction watermark read that precedes the advance-and-upsert:
/// selects the current encoded `position` for a table under the dialect's row-lock
/// suffix, so the read-advance-upsert is serialised per *existing* progress row
/// (a not-yet-inserted row cannot be locked — see [`watermark_upsert`]). Binds
/// `(table_name)`. The resume point is the encoded `position` value, not the
/// integer sort key — the migration has already run in this transaction, so the
/// column is present. Runs *inside* the write
/// transaction (unlike [`stored_position_query`], the lock-free read used by
/// `stored_position`).
pub(super) fn locked_progress_select(dialect: &dyn Dialect) -> String {
    format!(
        "SELECT position FROM {PROGRESS_TABLE} WHERE table_name = {}{}",
        dialect.placeholder(1),
        dialect.progress_row_lock()
    )
}

/// The read-side resume-point lookup: the encoded `position` for a table, or no
/// row when nothing has been written. Binds `(table_name)`. Read-only, outside any
/// transaction (the locked in-transaction twin is [`locked_progress_select`]). On a
/// pre-migration archive the `position` column is absent, or an old binary may have
/// left it NULL; in both cases the caller falls back to [`last_block_query`] and
/// decodes the decimal text.
pub(super) fn stored_position_query(dialect: &dyn Dialect) -> String {
    format!(
        "SELECT position FROM {PROGRESS_TABLE} WHERE table_name = {}",
        dialect.placeholder(1)
    )
}

/// The read-side `last_block` fallback lookup: the retained integer sort key for a
/// table, or no row when nothing has been written. Binds `(table_name)`. Used only
/// when the encoded `position` column is absent (a pre-migration archive) or NULL,
/// so an old block archive resumes at the same block before its first write.
pub(super) fn last_block_query(dialect: &dyn Dialect) -> String {
    format!(
        "SELECT last_block FROM {PROGRESS_TABLE} WHERE table_name = {}",
        dialect.placeholder(1)
    )
}

/// The replay `SELECT`: every event column for blocks up to (and including) the
/// `<= placeholder` bound, ordered by block then the dialect's tie-breaker so
/// one block's rows come back in (approximately) write order — see
/// [`Dialect::tiebreak`] for what each backend's column actually guarantees.
/// Binds `(up_to_sort_key)`.
pub(super) fn replay_query(schema: &TableSchema, dialect: &dyn Dialect) -> String {
    format!(
        "SELECT {} FROM {} WHERE {BLOCK_NUMBER_COLUMN} <= {} \
         ORDER BY {BLOCK_NUMBER_COLUMN} ASC, {} ASC",
        select_column_list(schema),
        quote_ident(&schema.table),
        dialect.placeholder(1),
        dialect.tiebreak()
    )
}

/// The serving layer's paged, block-range query: all columns for `table` in the
/// inclusive `[from, to]` block range, ascending, with the dialect's tie-breaker
/// and `LIMIT`/`OFFSET`. Binds `(from_block, to_block, limit, offset)`. The
/// read-side twin of [`replay_query`] — both depend only on the same two dialect
/// facts (placeholder, tie-breaker).
#[cfg(feature = "serving")]
pub(crate) fn range_query(table: &str, dialect: &dyn Dialect) -> String {
    let block = quote_ident(BLOCK_NUMBER_COLUMN);
    format!(
        "SELECT * FROM {} WHERE {block} BETWEEN {} AND {} \
         ORDER BY {block} ASC, {} ASC LIMIT {} OFFSET {}",
        quote_ident(table),
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.tiebreak(),
        dialect.placeholder(3),
        dialect.placeholder(4),
    )
}

/// Reject a row whose value count does not match the schema before any bind.
///
/// A short argument list would silently desync columns from values (sqlx binds
/// `NULL` for the gap), corrupting the table rather than erroring; both stores
/// bail here instead, rolling their transaction back.
pub(super) fn check_row_shape(schema: &TableSchema, row: &Row) -> Result<()> {
    if row.0.len() != schema.columns.len() {
        anyhow::bail!(
            "row has {} values but table {:?} has {} columns",
            row.0.len(),
            schema.table,
            schema.columns.len()
        );
    }
    Ok(())
}

/// Decode backend rows into [`Row`]s by applying `decode` to each column in
/// schema order. The per-backend cell extraction is supplied by `decode`; the
/// loop that assembles rows is identical across stores and lives only here.
pub(super) fn collect_rows<R>(
    rows: &[R],
    schema: &TableSchema,
    decode: impl Fn(&R, usize, SqlType) -> Result<SqlValue>,
) -> Result<Vec<Row>> {
    let mut out = Vec::with_capacity(rows.len());
    for r in rows {
        let mut values = Vec::with_capacity(schema.columns.len());
        for (idx, c) in schema.columns.iter().enumerate() {
            values.push(decode(r, idx, c.ty)?);
        }
        out.push(Row(values));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::dialect::SqliteDialect;

    fn schema() -> TableSchema {
        TableSchema::new("transfer")
            .col("from", SqlType::Text)
            .col("amount", SqlType::Numeric)
    }

    #[test]
    fn replay_query_uses_dialect_placeholder_and_tiebreak() {
        let q = replay_query(&schema(), &SqliteDialect);
        assert!(q.contains("<= ?"), "{q}");
        assert!(q.ends_with("ASC, rowid ASC"), "{q}");
    }

    #[test]
    fn insert_statement_emits_one_placeholder_per_column() {
        // block_number + 2 event columns = 3 placeholders.
        let q = insert_statement(&schema(), &SqliteDialect, 1);
        assert!(q.contains("VALUES (?, ?, ?)"), "{q}");
    }

    #[test]
    fn insert_statement_emits_one_placeholder_group_per_row() {
        let q = insert_statement(&schema(), &SqliteDialect, 3);
        assert!(q.contains("VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)"), "{q}");
    }

    // PostgreSQL placeholders are positional, so numbering must continue
    // across the rows of one multi-row insert.
    #[cfg(feature = "postgres")]
    #[test]
    fn insert_statement_numbers_placeholders_across_rows() {
        use crate::persistence::dialect::PgDialect;
        let q = insert_statement(&schema(), &PgDialect, 2);
        assert!(q.contains("VALUES ($1, $2, $3), ($4, $5, $6)"), "{q}");
    }

    // The fresh progress DDL has three columns, including the authoritative
    // encoded `position TEXT NOT NULL`.
    #[test]
    fn create_progress_table_has_three_columns_including_position() {
        let ddl = create_progress_table(&SqliteDialect);
        assert!(ddl.contains("table_name TEXT PRIMARY KEY"), "{ddl}");
        // The retained sort key keeps the SQLite integer type.
        assert!(ddl.contains("last_block INTEGER NOT NULL"), "{ddl}");
        assert!(ddl.contains("position TEXT NOT NULL"), "{ddl}");
    }

    // The lazy migration adds the column, then converts every integer last_block
    // into its encoded BlockPosition via a pure-SQL CAST. These statements are
    // dialect-independent.
    #[test]
    fn add_column_and_cast_backfill_compose() {
        assert_eq!(
            add_position_column(),
            "ALTER TABLE _artemis_progress ADD COLUMN position TEXT"
        );
        assert_eq!(
            backfill_position_from_last_block(),
            "UPDATE _artemis_progress SET position = CAST(last_block AS TEXT) WHERE position IS NULL"
        );
    }

    #[test]
    fn watermark_upsert_stores_both_columns_verbatim() {
        // No SQL MAX/GREATEST: both bound values are already advanced in Rust, so
        // the conflict clause copies `excluded.*` as-is — for BlockPosition the
        // `position` text is the same decimal as `last_block`.
        let q = watermark_upsert(&SqliteDialect);
        assert!(
            q.contains("(table_name, last_block, position) VALUES (?, ?, ?)"),
            "{q}"
        );
        assert!(q.contains("last_block = excluded.last_block"), "{q}");
        assert!(q.contains("position = excluded.position"), "{q}");
        assert!(!q.contains("MAX"), "{q}");
        assert!(!q.contains("GREATEST"), "{q}");
    }

    #[test]
    fn locked_progress_select_reads_position_without_lock_suffix_on_sqlite() {
        // The in-transaction resume read is the encoded `position` column, and
        // SQLite serialises writes at the database level: no ` FOR UPDATE`.
        let q = locked_progress_select(&SqliteDialect);
        assert!(
            q.starts_with("SELECT position FROM _artemis_progress WHERE table_name = ?"),
            "{q}"
        );
        assert!(!q.contains("FOR UPDATE"), "{q}");
    }

    #[test]
    fn stored_position_query_reads_position_column() {
        // The read-side resume lookup projects the encoded `position`, falling back
        // to `last_block_query` only when the column is absent or NULL.
        let q = stored_position_query(&SqliteDialect);
        assert!(
            q.starts_with("SELECT position FROM _artemis_progress WHERE table_name = ?"),
            "{q}"
        );
        let fallback = last_block_query(&SqliteDialect);
        assert!(
            fallback.starts_with("SELECT last_block FROM _artemis_progress WHERE table_name = ?"),
            "{fallback}"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn create_progress_table_uses_bigint_last_block_on_postgres() {
        use crate::persistence::dialect::PgDialect;
        let ddl = create_progress_table(&PgDialect);
        // The retained sort key is BIGINT on PostgreSQL; the encoded column is TEXT.
        assert!(ddl.contains("last_block BIGINT NOT NULL"), "{ddl}");
        assert!(ddl.contains("position TEXT NOT NULL"), "{ddl}");
    }

    #[cfg(feature = "serving")]
    #[test]
    fn range_query_binds_four_positions_with_tiebreak() {
        let q = range_query("transfer", &SqliteDialect);
        assert!(q.contains("BETWEEN ? AND ?"), "{q}");
        assert!(q.contains("rowid ASC LIMIT ? OFFSET ?"), "{q}");
    }
}
