use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;

use super::filter_map::subscribe_filter_mapped;

/// `Map` is a wrapper around a [Collector] that maps outgoing
/// events to a different type: a [`FilterMap`](super::FilterMap)
/// that never discards.
pub struct Map<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> Map<E, F> {
    /// Creates a new `Map` wrapping `collector` with the mapping function `f`.
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for Map<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> E2 + Send + Sync + Clone + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E2>> {
        let f = self.f.clone();
        subscribe_filter_mapped(&*self.collector, move |event| Some(f(event))).await
    }
}
