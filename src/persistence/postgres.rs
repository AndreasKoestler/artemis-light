//! The PostgreSQL backend: a [`SqlStore`] over a PostgreSQL pool with the
//! [`PgDialect`]. Only the connection setup is PostgreSQL-specific; the
//! `write_block` / `last_block` / `replay` orchestration lives once in
//! [`SqlStore`](super::SqlStore), and the dialect-only differences ($N
//! placeholders, `GREATEST` watermark, SQLSTATE `42P01`, `ctid` tie-breaker,
//! the column-type mapping) live in [`PgDialect`]. Compiled only under the
//! `postgres` feature (postgres-store.FEATURE.1).

use std::str::FromStr;

use anyhow::Result;
use sqlx::Postgres;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};

use super::dialect::PgDialect;
use super::sqlstore::SqlStore;

/// A PostgreSQL-backed [`Store`](super::Store).
pub type PostgresStore = SqlStore<Postgres, PgDialect>;

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
        Ok(SqlStore::new(pool, PgDialect))
    }
}

#[cfg(test)]
mod tests {
    use super::super::schema::{SqlType, TableSchema};

    // Reserved-name rejection (postgres-store.PGSTORE.7) is backend-agnostic:
    // `Record`/`Persisted` call `TableSchema::ensure_no_reserved_names` on the
    // user's schema BEFORE any Store sees it (persisted.rs / record.rs). The
    // generic `SqlStore` deliberately does NOT re-check inside `write_block` —
    // both because that would duplicate the upstream guard and because the
    // schema reaching the Store legitimately carries the reserved `_payload`
    // column. This test pins the shared guard the store relies on.
    #[test]
    fn shared_reserved_name_guard_rejects_reserved_identifiers() {
        assert!(
            TableSchema::new("_artemis_progress")
                .col("value", SqlType::Text)
                .ensure_no_reserved_names()
                .is_err()
        );
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
        assert!(
            TableSchema::new("evt")
                .col("value", SqlType::Text)
                .ensure_no_reserved_names()
                .is_ok()
        );
    }
}
