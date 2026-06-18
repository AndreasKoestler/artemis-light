//! Dialect-parameterised SQL shaping shared by the generic
//! [`SqlStore`](super::SqlStore) and the read serving backends.
//!
//! Every query whose shape is identical across backends lives here; the parts
//! that genuinely differ are supplied by a [`Dialect`]: the placeholder syntax,
//! the intra-block tie-breaker, the column-type keywords, and the
//! monotonic-watermark upsert. The two backends therefore cannot drift apart on
//! the parts they share (postgres-store.PARITY.1). Per-backend value binding and
//! cell decoding are *not* here — they ride sqlx's per-database types and live
//! in [`SqlStore`](super::SqlStore).

use anyhow::Result;

use super::dialect::Dialect;
use super::schema::{
    BLOCK_NUMBER_COLUMN, PROGRESS_TABLE, Row, SqlType, SqlValue, TableSchema, quote_ident,
};

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
/// takes the dialect's integer type (INTEGER / BIGINT).
pub(super) fn create_progress_table(dialect: &dyn Dialect) -> String {
    format!(
        "CREATE TABLE IF NOT EXISTS {PROGRESS_TABLE} \
         (table_name TEXT PRIMARY KEY, last_block {} NOT NULL)",
        dialect.column_type(SqlType::Integer)
    )
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

/// The per-row `INSERT`, with one dialect placeholder per bound column in
/// [`insert_column_names`] order.
pub(super) fn insert_statement(schema: &TableSchema, dialect: &dyn Dialect) -> String {
    let col_names = insert_column_names(schema);
    let placeholders = (1..=col_names.len())
        .map(|i| dialect.placeholder(i))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "INSERT INTO {} ({}) VALUES ({})",
        quote_ident(&schema.table),
        col_names.join(", "),
        placeholders
    )
}

/// The watermark upsert: advance `last_block` for a table, monotonically, in the
/// same transaction as its rows. Placeholders bind `(table_name, last_block)`;
/// the monotonic max expression is the dialect's.
pub(super) fn watermark_upsert(dialect: &dyn Dialect) -> String {
    format!(
        "INSERT INTO {PROGRESS_TABLE} (table_name, last_block) VALUES ({}, {}) \
         ON CONFLICT (table_name) DO UPDATE SET {}",
        dialect.placeholder(1),
        dialect.placeholder(2),
        dialect.monotonic_watermark_set()
    )
}

/// The watermark lookup: the last processed block for a table, or no row when
/// nothing has been written. Binds `(table_name)`.
pub(super) fn last_block_query(dialect: &dyn Dialect) -> String {
    format!(
        "SELECT last_block FROM {PROGRESS_TABLE} WHERE table_name = {}",
        dialect.placeholder(1)
    )
}

/// The replay `SELECT`: every event column for blocks up to (and including) the
/// `<= placeholder` bound, ordered by block then the dialect's tie-breaker for a
/// stable, deterministic replay. Binds `(to_block)`.
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
/// bail here instead, rolling their transaction back (postgres-store.PGSTORE.6).
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
        let q = insert_statement(&schema(), &SqliteDialect);
        assert!(q.contains("VALUES (?, ?, ?)"), "{q}");
    }

    #[cfg(feature = "serving")]
    #[test]
    fn range_query_binds_four_positions_with_tiebreak() {
        let q = range_query("transfer", &SqliteDialect);
        assert!(q.contains("BETWEEN ? AND ?"), "{q}");
        assert!(q.contains("rowid ASC LIMIT ? OFFSET ?"), "{q}");
    }
}
