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

    /// The intra-block tie-breaker column ordering rows within one block:
    /// `rowid` for SQLite, `ctid` for PostgreSQL. What each guarantees
    /// differs — see the per-backend impls.
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
    /// row lock for an injected multi-connection pool.
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
    /// predates the encoded `position` column and must be migrated. SQLite
    /// matches the driver message (`no such column`); PostgreSQL matches
    /// SQLSTATE `42703`.
    ///
    /// The default returns `false`, so a third-party [`Dialect`] impl keeps
    /// compiling; such a backend simply never triggers the lazy migration — it is
    /// expected to have been created with the current (encoded-position) schema.
    fn is_undefined_column(&self, _err: &sqlx::Error) -> bool {
        false
    }

    /// Whether `err` is the backend's benign "already exists" signal from two
    /// writers racing the same DDL — the store's `CREATE TABLE IF NOT EXISTS`
    /// or the lazy migration's `ADD COLUMN` on a shared multi-connection pool.
    /// PostgreSQL matches SQLSTATEs `42P07` (duplicate_table), `42701`
    /// (duplicate_column), and `23505` (`CREATE TABLE IF NOT EXISTS` can lose
    /// its internal catalog race as a unique_violation — a known PostgreSQL
    /// gap). The store treats a write that failed this way as "already
    /// exists" and retries it once instead of going permanently unhealthy.
    ///
    /// The default returns `false` (never benign), correct for any
    /// single-writer backend such as SQLite.
    fn is_duplicate_object(&self, _err: &sqlx::Error) -> bool {
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
        // `ctid` approximates insertion order on an append-only table between
        // table rewrites — good enough to keep one block's rows in write
        // order. It is *not* a durable ordering key: VACUUM FULL / CLUSTER
        // renumber tuples, and a rolled-back insert can leave a later row
        // with a lower ctid.
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
        // advance happens in `Position::advance` in Rust, not in SQL.
        " FOR UPDATE"
    }

    fn is_undefined_table(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42P01"))
    }

    fn is_undefined_column(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e) if e.code().as_deref() == Some("42703"))
    }

    fn is_duplicate_object(&self, err: &sqlx::Error) -> bool {
        matches!(err, sqlx::Error::Database(e)
            if matches!(e.code().as_deref(), Some("42P07" | "42701" | "23505")))
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

    /// A minimal [`sqlx::error::DatabaseError`] carrying just a SQLSTATE code,
    /// so classification can be tested against real `sqlx::Error` shapes
    /// without a live database.
    #[derive(Debug)]
    struct FakeDbError(&'static str);

    impl std::fmt::Display for FakeDbError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "fake database error (SQLSTATE {})", self.0)
        }
    }

    impl std::error::Error for FakeDbError {}

    impl sqlx::error::DatabaseError for FakeDbError {
        fn message(&self) -> &str {
            "fake database error"
        }

        fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
            Some(std::borrow::Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> sqlx::error::ErrorKind {
            sqlx::error::ErrorKind::Other
        }
    }

    fn db_error(code: &'static str) -> sqlx::Error {
        sqlx::Error::Database(Box::new(FakeDbError(code)))
    }

    // Two collectors sharing one injected multi-connection pool can race on
    // first write: concurrent `CREATE TABLE IF NOT EXISTS` raises 42P07 or
    // 23505 (a known PostgreSQL gap), and the lazy migration's ADD COLUMN
    // raises 42701. All three are benign "already exists" verdicts for the
    // DDL path; anything else must not be classified.
    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_classifies_ddl_race_sqlstates_as_duplicate_object() {
        let d = PgDialect;
        assert!(d.is_duplicate_object(&db_error("42P07")), "duplicate_table");
        assert!(
            d.is_duplicate_object(&db_error("42701")),
            "duplicate_column"
        );
        assert!(
            d.is_duplicate_object(&db_error("23505")),
            "unique_violation"
        );
        // Not a race marker: undefined table / syntax error / non-database.
        assert!(!d.is_duplicate_object(&db_error("42P01")));
        assert!(!d.is_duplicate_object(&db_error("42601")));
        assert!(!d.is_duplicate_object(&sqlx::Error::RowNotFound));
    }

    // SQLite keeps the default: its single-writer pools cannot race on DDL.
    #[test]
    fn sqlite_never_classifies_a_duplicate_object() {
        assert!(!SqliteDialect.is_duplicate_object(&db_error("42P07")));
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
