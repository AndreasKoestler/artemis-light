//! The SQL-**Dialect** seam: the small, stateless set of SQL-text facts that
//! differ between the SQLite and PostgreSQL backends. See `CONTEXT.md`
//! ("Dialect"). One unit-struct adapter per backend; the query-shaping
//! functions in [`query`](super::query), the generic write
//! [`SqlStore`](super::SqlStore), and the read serving backends all consume the
//! same `Dialect` so the two sides can never drift on a placeholder or
//! tie-breaker.
//!
//! A Dialect substitutes tokens into an otherwise-shared query; it does **not**
//! know how a backend enumerates its own tables — that is the serving layer's
//! Catalog concern, a separate seam (see ADR-0002).

use super::schema::SqlType;

/// The SQL-text facts that differ between storage backends.
pub trait Dialect: Send + Sync {
    /// The positional placeholder for the `n`th bound parameter (1-based): `?`
    /// for SQLite (position-independent), `$n` for PostgreSQL.
    fn placeholder(&self, n: usize) -> String;

    /// The stable intra-block tie-breaker column for a deterministic order:
    /// `rowid` for SQLite, `ctid` for PostgreSQL.
    fn tiebreak(&self) -> &'static str;

    /// The `CREATE TABLE` column-type keyword `ty` maps to. The implicit
    /// `block_number` column's type falls out of
    /// `column_type(SqlType::Integer)` (INTEGER vs BIGINT).
    ///
    /// `Numeric` maps to `TEXT` on PostgreSQL (not `NUMERIC`): a `Numeric` value
    /// round-trips as [`SqlValue::Text`](super::SqlValue::Text), so a `TEXT`
    /// column makes PostgreSQL replay produce the identical value without a
    /// decimal dependency.
    fn column_type(&self, ty: SqlType) -> &'static str;

    /// The row-lock suffix appended to the in-transaction progress `SELECT` so
    /// the read-advance-upsert of one table's watermark is serialised across
    /// connections: `` (empty) on SQLite, which serialises writes at the
    /// database level, and ` FOR UPDATE` on PostgreSQL, which needs an explicit
    /// row lock for an injected multi-connection pool — [position-trait.ATOMIC.1].
    ///
    /// The default returns the empty string, so a third-party [`Dialect`] impl
    /// keeps compiling and simply omits the lock (correct for any single-writer
    /// backend).
    fn progress_row_lock(&self) -> &'static str {
        ""
    }

    /// Whether `err` is the backend's "table does not exist yet" signal — the
    /// marker that nothing has ever been written for a table. SQLite matches the
    /// driver message; PostgreSQL matches SQLSTATE `42P01`.
    fn is_undefined_table(&self, err: &sqlx::Error) -> bool;

    /// Whether `err` is the backend's "column does not exist" signal — the marker,
    /// on the one-shot migration probe (`SELECT position …`), that this archive
    /// predates the encoded `position` column and must be migrated
    /// [position-trait.MIGRATE.1]. SQLite matches the driver message
    /// (`no such column`); PostgreSQL matches SQLSTATE `42703`.
    ///
    /// The default returns `false`, so a third-party [`Dialect`] impl keeps
    /// compiling; such a backend simply never triggers the lazy migration — it is
    /// expected to have been created with the current (encoded-position) schema.
    fn is_undefined_column(&self, _err: &sqlx::Error) -> bool {
        false
    }
}

/// The SQLite [`Dialect`] adapter.
pub struct SqliteDialect;

impl Dialect for SqliteDialect {
    fn placeholder(&self, _n: usize) -> String {
        "?".to_string()
    }

    fn tiebreak(&self) -> &'static str {
        "rowid"
    }

    fn column_type(&self, ty: SqlType) -> &'static str {
        ty.sql()
    }

    fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.message().contains("no such table"))
    }

    fn is_undefined_column(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.message().contains("no such column"))
    }
}

/// The PostgreSQL [`Dialect`] adapter. Compiled only under the `postgres`
/// feature.
#[cfg(feature = "postgres")]
pub struct PgDialect;

#[cfg(feature = "postgres")]
impl Dialect for PgDialect {
    fn placeholder(&self, n: usize) -> String {
        format!("${n}")
    }

    fn tiebreak(&self) -> &'static str {
        "ctid"
    }

    fn column_type(&self, ty: SqlType) -> &'static str {
        match ty {
            SqlType::Integer => "BIGINT",
            SqlType::Real => "DOUBLE PRECISION",
            SqlType::Text => "TEXT",
            SqlType::Blob => "BYTEA",
            SqlType::Numeric => "TEXT",
        }
    }

    fn progress_row_lock(&self) -> &'static str {
        // Serialise the read-advance-upsert of one table's watermark across the
        // connections of an injected multi-connection pool — the monotonic
        // advance now lives in `Position::advance` in Rust, not in SQL.
        " FOR UPDATE"
    }

    fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42P01"))
    }

    fn is_undefined_column(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42703"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_placeholder_is_position_independent() {
        let d = SqliteDialect;
        assert_eq!(d.placeholder(1), "?");
        assert_eq!(d.placeholder(4), "?");
        assert_eq!(d.tiebreak(), "rowid");
        // block_number type falls out of column_type(Integer).
        assert_eq!(d.column_type(SqlType::Integer), "INTEGER");
        assert_eq!(d.column_type(SqlType::Numeric), "NUMERIC");
        // SQLite serialises writes at the database level, so no row-lock suffix.
        assert_eq!(d.progress_row_lock(), "");
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_placeholder_is_positional_and_numeric_is_text() {
        let d = PgDialect;
        assert_eq!(d.placeholder(1), "$1");
        assert_eq!(d.placeholder(4), "$4");
        assert_eq!(d.tiebreak(), "ctid");
        assert_eq!(d.column_type(SqlType::Integer), "BIGINT");
        // Numeric stores as TEXT so replay round-trips identically to SQLite.
        assert_eq!(d.column_type(SqlType::Numeric), "TEXT");
        // PostgreSQL needs an explicit row lock for an injected multi-connection pool.
        assert_eq!(d.progress_row_lock(), " FOR UPDATE");
    }
}
