use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::stream::{select, select_all};

/// Interleaves two [Collector]s into one composite Collector: events arrive in
/// whichever order the sources produce them.
///
/// Both sources subscribe when the composite subscribes; either failing fails
/// the whole subscribe, so the failure reaches the Reconnect Policy instead of
/// vanishing. The composite stream ends only when *both* source streams have
/// ended. To the Engine the composite is one Collector — one Collector Driver,
/// one Reconnect Policy, one lifecycle shared by both sources.
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
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let this_stream = self.this.subscribe().await?;
        let other_stream = self.other.subscribe().await?;
        Ok(Box::pin(select(this_stream, other_stream)))
    }
}

/// [`Merge`] over a runtime-sized set of sources; see [`merge_all`].
pub struct MergeAll<E> {
    sources: Vec<Box<dyn Collector<E>>>,
}

/// Interleaves every source into one composite Collector, with the same
/// contract as [`Merge`]: eager subscribe, any failure fails the whole
/// subscribe, the composite stream ends only when every source stream has
/// ended, and the sources share one lifecycle (one Collector Driver, one
/// Reconnect Policy).
pub fn merge_all<E>(sources: Vec<Box<dyn Collector<E>>>) -> MergeAll<E> {
    MergeAll { sources }
}

#[async_trait]
impl<E: Send + Sync + 'static> Collector<E> for MergeAll<E> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let mut streams = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            streams.push(source.subscribe().await?);
        }
        Ok(Box::pin(select_all(streams)))
    }
}
