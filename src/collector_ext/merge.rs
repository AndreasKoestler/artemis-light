use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::{StreamExt, stream::select};
use jsonrpsee::core::DeserializeOwned;

/// Merges two [Collector]s into a single stream that interleaves events from both.
pub struct Merge<C1, C2> {
    this: C1,
    other: C2,
}

impl<C1, C2> Merge<C1, C2> {
    /// Creates a new `Merge` that interleaves events from `this` and `other`.
    pub fn new(this: C1, other: C2) -> Self {
        Self { this, other }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for Merge<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + DeserializeOwned + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let this_stream = self.this.subscribe().await?;
        let other_stream = self.other.subscribe().await?;
        let merged = Box::pin(select(this_stream, other_stream)) as CollectorStream<'_, E>;
        Ok(Box::pin(merged))
    }
}

#[async_trait]
impl<E: 'static, C: Collector<E>> Collector<E> for Vec<Box<C>> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let stream = futures::stream::iter(self.iter())
            .then(|collector| collector.subscribe())
            .filter_map(|result| async { result.ok() })
            .flatten();
        Ok(Box::pin(stream))
    }
}
