//! The [`Store`] trait: a SQL backend for indexed events.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::position::{BlockPosition, Position};
use super::schema::{Row, TableSchema};

/// A storage backend that records indexed events, one table per event type.
///
/// Implementations persist a whole position-group's rows transactionally and
/// track the last processed [`Position`], so a subscription can be resumed
/// without gaps or double-writes. The store is generic over the position type
/// `P` (a block number, a queue offset, a `(time, seen-set)` frontier) rather
/// than a bare `u64` block number — [position-trait.STORE.1]. The default
/// `P = BlockPosition` keeps `dyn Store` / `S: Store` meaning
/// `Store<BlockPosition>`, so the EVM path stays source-compatible
/// [position-trait.PARITY.3].
#[async_trait]
pub trait Store<P: Position = BlockPosition>: Send + Sync {
    /// Persist every row emitted at `position` for `schema`'s table, creating
    /// the table if needed, and advance the stored position — all in a single
    /// transaction. Replaces the former `write_block` — [position-trait.STORE.1-1].
    async fn write(&self, schema: &TableSchema, position: P, rows: Vec<Row>) -> Result<()>;

    /// The last processed [`Position`] for `table`, or `None` if nothing is
    /// stored. Replaces the former `last_block` — [position-trait.STORE.1-2].
    async fn stored_position(&self, table: &str) -> Result<Option<P>>;

    /// Replay stored rows for `schema`'s table with `sort_key <= up_to.sort_key()`,
    /// in ascending sort-key order. Returns an empty vec if the table does not
    /// exist. Replaces the former `u64`-bounded `replay` — [position-trait.STORE.1-3].
    async fn replay(&self, schema: &TableSchema, up_to: P) -> Result<Vec<Row>>;
}

/// Blanket impl so a shared [`Arc<T>`] can be used wherever a [`Store<P>`] is
/// expected — handy for sharing one store across collectors and assertions.
/// `?Sized` admits `Arc<dyn Store>` — [position-trait.PARITY.3].
#[async_trait]
impl<P: Position, T: Store<P> + ?Sized> Store<P> for Arc<T> {
    async fn write(&self, schema: &TableSchema, position: P, rows: Vec<Row>) -> Result<()> {
        (**self).write(schema, position, rows).await
    }

    async fn stored_position(&self, table: &str) -> Result<Option<P>> {
        (**self).stored_position(table).await
    }

    async fn replay(&self, schema: &TableSchema, up_to: P) -> Result<Vec<Row>> {
        (**self).replay(schema, up_to).await
    }
}
