use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use futures::StreamExt;

/// `FilterMap` is a wrapper around a [Collector] that filter-maps
/// outgoing events, discarding `None` results and unwrapping `Some`.
pub struct FilterMap<E, F> {
    collector: Box<dyn Collector<E>>,
    f: F,
}

impl<E, F> FilterMap<E, F> {
    /// Creates a new `FilterMap` wrapping `collector` with the filter-map function `f`.
    pub fn new(collector: Box<dyn Collector<E>>, f: F) -> Self {
        Self { collector, f }
    }
}

/// Subscribe `collector` and adapt its stream through `f`, discarding `None`
/// results and unwrapping `Some` — the one stream-adaptation site shared by
/// [`FilterMap`] and [`Map`](super::Map) (a map is a filter-map that never
/// discards).
pub(super) async fn subscribe_filter_mapped<'a, E1, E2, F>(
    collector: &'a dyn Collector<E1>,
    f: F,
) -> Result<CollectorStream<'a, E2>>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + Clone + 'static,
{
    let stream = collector.subscribe().await?;
    let stream = stream.filter_map(move |event| {
        let f = f.clone();
        async move { f(event) }
    });
    Ok(Box::pin(stream))
}

#[async_trait]
impl<E1, E2, F> Collector<E2> for FilterMap<E1, F>
where
    E1: Send + Sync + 'static,
    E2: Send + Sync + 'static,
    F: Fn(E1) -> Option<E2> + Send + Sync + Clone + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E2>> {
        subscribe_filter_mapped(&*self.collector, self.f.clone()).await
    }
}
