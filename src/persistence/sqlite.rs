//! The SQLite backend: a [`SqlStore`] over an SQLite pool with the
//! [`SqliteDialect`]. Only the connection tuning is SQLite-specific; the
//! `write` / `stored_position` / `replay` orchestration lives once in
//! [`SqlStore`](super::SqlStore).

use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use sqlx::Sqlite;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};

use super::dialect::SqliteDialect;
use super::sqlstore::SqlStore;

/// A SQLite-backed [`Store`](super::Store).
pub type SqliteStore = SqlStore<Sqlite, SqliteDialect>;

impl SqliteStore {
    /// Open (creating if missing) a SQLite database at `url`.
    ///
    /// Use `"sqlite::memory:"` for an ephemeral in-memory database. A single
    /// connection is used so an in-memory database is shared across calls and
    /// every write sees a consistent view; the pool never retires it (see
    /// [`pool_options`]), because an in-memory database dies with its
    /// connection.
    ///
    /// File databases run in WAL journal mode with `synchronous = NORMAL` and
    /// a 5s busy timeout: the default rollback journal answers any concurrent
    /// access with an immediate `SQLITE_BUSY`, and a single failed write
    /// permanently halts persistence (by design, to keep the stored height a
    /// gap-free prefix) — so a stray reader must wait, not kill the archive.
    /// In-memory databases ignore the journal mode.
    pub async fn connect(url: &str) -> Result<Self> {
        let opts = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let pool = pool_options().connect_with(opts).await?;
        Ok(SqlStore::new(pool, SqliteDialect))
    }
}

/// The single-connection pool tuning: exactly one connection, never retired.
/// sqlx's defaults (min_connections 0, a 10-minute idle timeout, a 30-minute
/// max lifetime) reap an idle connection — and for `sqlite::memory:` the
/// database lives *in* that connection, so a reaped connection would reopen as
/// a fresh empty database and silently destroy the archive. Pinning the one
/// connection is harmless for file databases too: closing a pool's only
/// connection saves nothing.
fn pool_options() -> SqlitePoolOptions {
    SqlitePoolOptions::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// sqlx's pool defaults (min_connections 0, a 10-minute idle timeout, a
    /// 30-minute max lifetime) retire an idle connection — and for
    /// `sqlite::memory:` the database lives *in* that one connection, so a
    /// retired connection reopens as a fresh empty database, silently
    /// destroying the archive. The pool must never retire its single
    /// connection.
    #[test]
    fn pool_never_retires_its_single_connection() {
        let opts = pool_options();
        assert_eq!(opts.get_max_connections(), 1);
        assert_eq!(opts.get_min_connections(), 1, "keep the connection open");
        assert_eq!(opts.get_idle_timeout(), None, "never reap on idleness");
        assert_eq!(opts.get_max_lifetime(), None, "never recycle on age");
    }
}
