//! The [`Store`] trait: a SQL backend for indexed events.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use super::schema::{Row, TableSchema};

/// A storage backend that records indexed events, one table per event type.
///
/// Implementations persist a whole block's rows transactionally and track the
/// last processed block, so a subscription can be resumed without gaps or
/// double-writes.
#[async_trait]
pub trait Store: Send + Sync {
    /// Persist every row emitted in `block` for `schema`'s table, creating the
    /// table if needed, and advance the last processed block — all in a single
    /// transaction.
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()>;

    /// The last processed block for `table`, or `None` if nothing is stored.
    async fn last_block(&self, table: &str) -> Result<Option<u64>>;

    /// Replay stored rows for `schema`'s table with `block_number <= to`, in
    /// ascending block order. Returns an empty vec if the table does not exist.
    async fn replay(&self, schema: &TableSchema, to: u64) -> Result<Vec<Row>>;
}

/// Blanket impl so a shared [`Arc<S>`] can be used wherever a [`Store`] is
/// expected — handy for sharing one store across collectors and assertions.
#[async_trait]
impl<T: Store + ?Sized> Store for Arc<T> {
    async fn write_block(&self, schema: &TableSchema, block: u64, rows: Vec<Row>) -> Result<()> {
        (**self).write_block(schema, block, rows).await
    }

    async fn last_block(&self, table: &str) -> Result<Option<u64>> {
        (**self).last_block(table).await
    }

    async fn replay(&self, schema: &TableSchema, to: u64) -> Result<Vec<Row>> {
        (**self).replay(schema, to).await
    }
}
