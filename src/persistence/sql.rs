//! Dialect-independent SQL shaping shared by the SQLite and PostgreSQL stores.
//!
//! [`PostgresStore`](super::PostgresStore) mirrors
//! [`SqliteStore`](super::SqliteStore) method-for-method; the differences are
//! dialect-only (placeholder syntax, the monotonic-watermark function, missing
//! -table classification). The pieces that carry *no* dialect difference —
//! which columns an insert names, the replay projection, the row-shape guard,
//! and the row→[`Row`] decode loop — live here so the two stores cannot drift
//! apart on the parts they genuinely share (postgres-store.PARITY.1).

use anyhow::Result;

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

/// The replay `SELECT`: every event column for blocks up to (and including) the
/// `<= {placeholder}` bound, ordered by block then `order_key` for a stable,
/// deterministic replay. The two dialect differences are passed in — the bound
/// placeholder (`?` for SQLite, `$1` for PostgreSQL) and the intra-block tie
/// breaker (`rowid` / `ctid`) — so the query shape itself lives in one place.
pub(super) fn replay_query(schema: &TableSchema, placeholder: &str, order_key: &str) -> String {
    format!(
        "SELECT {} FROM {} WHERE {BLOCK_NUMBER_COLUMN} <= {placeholder} \
         ORDER BY {BLOCK_NUMBER_COLUMN} ASC, {order_key} ASC",
        select_column_list(schema),
        quote_ident(&schema.table)
    )
}

/// The watermark lookup: the last processed block for `table`, or no row when
/// nothing has been written. `placeholder` is the only dialect difference
/// (`?` for SQLite, `$1` for PostgreSQL).
pub(super) fn last_block_query(placeholder: &str) -> String {
    format!("SELECT last_block FROM {PROGRESS_TABLE} WHERE table_name = {placeholder}")
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
