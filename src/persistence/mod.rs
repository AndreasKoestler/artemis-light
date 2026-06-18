//! Persistence for the event pipeline.
//!
//! A [`Store`](crate::persistence::Store) is a SQL backend (SQLite first) that
//! records indexed events one table per event type, plus the last processed
//! block. The [`Persisted`](crate::persistence::Persisted) wrapper turns any
//! block-aware collector into one that records every event it sees and, on
//! subscribe, replays the stored history before catching up to and following
//! the chain tip. Every row written to or replayed from the Store passes
//! through a [`Record`](crate::persistence::Record), the mapping between one
//! event type and its SQL rows.
//!
//! The backends share a single orchestration body,
//! [`SqlStore`](crate::persistence::SqlStore), parameterised
//! by a [`Dialect`](crate::persistence::Dialect) that supplies the SQL-text facts that differ between SQLite
//! and PostgreSQL (see `CONTEXT.md` and ADR-0002). `SqliteStore` and
//! `PostgresStore` are thin aliases over it.

mod dialect;
mod persisted;
#[cfg(feature = "postgres")]
mod postgres;
mod record;
mod schema;
mod sql;
mod sqlite;
mod sqlstore;
mod store;
mod writer;

#[cfg(feature = "postgres")]
pub use dialect::PgDialect;
pub use dialect::{Dialect, SqliteDialect};
pub use persisted::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
pub use record::Record;
pub use schema::*;
pub use sqlite::*;
pub use sqlstore::SqlStore;
pub use store::*;

// The serving layer (a sibling module) shapes its paged range query through the
// same dialect-aware SQL as the store, so identifier quoting, placeholders, and
// the tie-breaker can never diverge between writer and reader.
#[cfg(feature = "serving")]
pub(crate) use sql::range_query;
