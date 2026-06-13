use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;

/// Subscribes a primary [Collector], falling back to a secondary if the
/// primary's subscribe fails.
///
/// Tries `this` first; on `Ok` it returns that stream and **never subscribes
/// `other`** (no wasted connection — unlike [`Merge`](super::Merge), which
/// subscribes every source). On `this`'s subscribe error it logs and tries
/// `other`. The combinator is stateless and primary-preferring: every
/// (re)subscribe tries `this` first, so a recovered primary is picked back up
/// automatically. Mid-stream failover happens through the Reconnect Policy's
/// re-subscribe, not state here. To the Engine the composite is one Collector —
/// one Collector Driver, one Reconnect Policy, one lifecycle.
pub struct Fallback<C1, C2> {
    this: C1,
    other: C2,
}

impl<C1, C2> Fallback<C1, C2> {
    /// Creates a new `Fallback` preferring `this`, falling back to `other`.
    pub fn new(this: C1, other: C2) -> Self {
        Self { this, other }
    }
}

#[async_trait]
impl<C1, C2, E> Collector<E> for Fallback<C1, C2>
where
    C1: Collector<E> + Send + Sync + 'static,
    C2: Collector<E> + Send + Sync + 'static,
    E: Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        match self.this.subscribe().await {
            Ok(stream) => Ok(stream),
            Err(e) => {
                tracing::warn!("primary collector subscribe failed; falling back: {e:#}");
                self.other.subscribe().await
            }
        }
    }
}

/// [`Fallback`] over a runtime-sized, ordered set of sources; see [`fallback_all`].
pub struct FallbackAll<E> {
    sources: Vec<Box<dyn Collector<E>>>,
}

/// Tries each source in registration order, falling back to the next on a
/// subscribe error, with the same contract as [`Fallback`]: the first source
/// to subscribe wins and later sources are never subscribed; if every source
/// fails, the whole subscribe fails (feeding the Reconnect Policy). The
/// sources share one lifecycle (one Collector Driver, one Reconnect Policy).
pub fn fallback_all<E>(sources: Vec<Box<dyn Collector<E>>>) -> FallbackAll<E> {
    FallbackAll { sources }
}

#[async_trait]
impl<E: Send + Sync + 'static> Collector<E> for FallbackAll<E> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let mut last_err = anyhow::anyhow!("fallback_all has no sources; nothing to subscribe to");
        for source in &self.sources {
            match source.subscribe().await {
                Ok(stream) => return Ok(stream),
                Err(e) => {
                    tracing::warn!("collector subscribe failed; trying next fallback: {e:#}");
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }
}
