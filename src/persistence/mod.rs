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

mod persisted;
#[cfg(feature = "postgres")]
mod postgres;
mod record;
mod schema;
mod sqlite;
mod store;
mod writer;

pub use persisted::*;
#[cfg(feature = "postgres")]
pub use postgres::*;
pub use record::Record;
pub use schema::*;
pub use sqlite::*;
pub use store::*;
