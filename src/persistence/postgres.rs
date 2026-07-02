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

    /// Build a store over a caller-supplied `sqlx::PgPool`, wrapping it with the
    /// [`PgDialect`] via the shared [`SqlStore::new`](super::SqlStore) seam — the
    /// same seam [`connect`](Self::connect) uses, so both construction paths drive
    /// the identical `write_block` / `last_block` / `replay` orchestration
    /// (inject-pool.STORE.1, inject-pool.STORE.4). Compiled only under the
    /// `postgres` feature (inject-pool.SCOPE.1).
    ///
    /// This is a plain synchronous constructor: it performs no connect
    /// round-trip, no I/O, and no DDL, and accepts a pool of any connection count
    /// without capping or overriding it (inject-pool.STORE.2, inject-pool.STORE.5).
    ///
    /// # Pool ownership
    ///
    /// The pool is *borrowed*: the store holds a handle to it but never closes it
    /// and never reconfigures it (no `after_connect` hook, no session `SET`, no
    /// pool-option mutation). Dropping the store leaves the caller's pool open and
    /// usable; the pool's lifecycle stays the caller's
    /// (inject-pool.OWNERSHIP.1, inject-pool.OWNERSHIP.2).
    ///
    /// # Single writer / gap-free prefix
    ///
    /// Injecting a multi-connection pool does not by itself weaken the
    /// gap-free-prefix durability guarantee: the persistence pipeline drives one
    /// writer per stream, awaiting each `write_block` commit before the next, so a
    /// single stream's writes are never reordered regardless of pool size. This
    /// holds provided the caller does not point two persisting collectors at the
    /// same table on the same pool — serializing writes across collectors is the
    /// caller's responsibility, not enforced by this constructor
    /// (inject-pool.WRITER.1).
    ///
    /// # Tables created
    ///
    /// artemis-light lazily creates `_artemis_progress` (the per-table watermark
    /// bookkeeping table) plus one table per event type on the first `write_block`.
    /// These are created unqualified, landing in the pool's default `search_path`.
    /// If that default is not `public`, persistence still works, but the serving
    /// layer's introspection queries (which look under `table_schema = 'public'`)
    /// will not see them (inject-pool.SCHEMA_DOCS.1).
    ///
    /// # Deferred error surfacing
    ///
    /// Because construction does no I/O, connectivity and permission errors do not
    /// surface here. A pool pointing at an unreachable or misconfigured server
    /// still constructs a store successfully; the error appears at the first store
    /// operation instead — mirroring how [`connect`](Self::connect) surfaces the
    /// same failures at connect time (inject-pool.ERRORS.1).
    pub fn with_pool(pool: sqlx::PgPool) -> Self {
        SqlStore::new(pool, PgDialect)
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
