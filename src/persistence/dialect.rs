//! The SQL-**Dialect** seam: the small, stateless set of SQL-text facts that
//! differ between the SQLite and PostgreSQL backends. See `CONTEXT.md`
//! ("Dialect"). One unit-struct adapter per backend; the query-shaping
//! functions in [`sql`](super::sql), the generic write
//! [`SqlStore`](super::SqlStore), and the read serving backends all consume the
//! same `Dialect` so the two sides can never drift on a placeholder or
//! tie-breaker.
//!
//! A Dialect substitutes tokens into an otherwise-shared query; it does **not**
//! know how a backend enumerates its own tables — that is the serving layer's
//! Catalog concern, a separate seam (see ADR-0002).

#[cfg(feature = "postgres")]
use super::schema::PROGRESS_TABLE;
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

    /// The `ON CONFLICT (table_name) DO UPDATE SET …` assignment that keeps the
    /// watermark monotonic so it never regresses. SQLite uses `MAX(...)`;
    /// PostgreSQL uses `GREATEST(...)` and must qualify the existing row with
    /// the progress table name (`_artemis_progress`).
    fn monotonic_watermark_set(&self) -> String;

    /// Whether `err` is the backend's "table does not exist yet" signal — the
    /// marker that nothing has ever been written for a table. SQLite matches the
    /// driver message; PostgreSQL matches SQLSTATE `42P01`.
    fn is_undefined_table(&self, err: &sqlx::Error) -> bool;
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

    fn monotonic_watermark_set(&self) -> String {
        "last_block = MAX(last_block, excluded.last_block)".to_string()
    }

    fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.message().contains("no such table"))
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

    fn monotonic_watermark_set(&self) -> String {
        format!("last_block = GREATEST({PROGRESS_TABLE}.last_block, excluded.last_block)")
    }

    fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42P01"))
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
        assert!(d.monotonic_watermark_set().contains("GREATEST"));
        assert!(d.monotonic_watermark_set().contains("_artemis_progress"));
    }
}
