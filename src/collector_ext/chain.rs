use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

/// Delivers two [Collector]s' streams strictly in sequence: `second`'s events
/// are held back until `first`'s stream ends.
///
/// Both sources subscribe when the composite subscribes — eagerly, so a later
/// live source buffers at its source rather than missing events while the
/// earlier segment drains (the same head-buffering rationale as the Persisted
/// Collector's subscribe). That buffering is bounded by what the source
/// retains: a lossy source (e.g. a lagging broadcast channel) can still drop
/// events it produced while held back. Either subscribe failing fails the
/// whole subscribe, so the failure reaches the Reconnect Policy instead of
/// vanishing. To the Engine the composite is one Collector — one Collector
/// Driver, one Reconnect Policy, one lifecycle shared by both sources.
pub struct Chain<C1, C2> {
    first: C1,
    second: C2,
}

impl<C1, C2> Chain<C1, C2> {
    /// Creates a new `Chain` delivering all of `first`, then all of `second`.
    pub fn new(first: C1, second: C2) -> Self {
        Self { first, second }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for Chain<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let first = self.first.subscribe().await?;
        let second = self.second.subscribe().await?;
        Ok(Box::pin(first.chain(second)))
    }
}

/// [`Chain`] over a runtime-sized set of sources; see [`chain_all`].
pub struct ChainAll<E> {
    sources: Vec<Box<dyn Collector<E>>>,
}

/// Delivers every source's stream strictly in registration order, with the
/// same contract as [`Chain`]: eager subscribe (later sources buffer at their
/// source while earlier segments drain), any failure fails the whole
/// subscribe, and the sources share one lifecycle (one Collector Driver, one
/// Reconnect Policy).
pub fn chain_all<E>(sources: Vec<Box<dyn Collector<E>>>) -> ChainAll<E> {
    ChainAll { sources }
}

#[async_trait]
impl<E: Send + Sync + 'static> Collector<E> for ChainAll<E> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let mut streams = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            streams.push(source.subscribe().await?);
        }
        // The sources are already subscribed, so flattening their streams in
        // order is sequential delivery, not lazy subscription.
        Ok(Box::pin(futures::stream::iter(streams).flatten()))
    }
}
