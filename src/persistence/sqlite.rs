//! The SQLite backend: a [`SqlStore`] over an SQLite pool with the
//! [`SqliteDialect`]. Only the connection tuning is SQLite-specific; the
//! `write_block` / `last_block` / `replay` orchestration lives once in
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
    /// every write sees a consistent view.
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
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        Ok(SqlStore::new(pool, SqliteDialect))
    }
}
