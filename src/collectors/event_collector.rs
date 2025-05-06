use crate::types::{Collector, CollectorStream};
use alloy::{contract::Event, providers::Provider, sol_types::SolEvent};
use anyhow::Result;
use async_trait::async_trait;
use tokio_stream::StreamExt;

/// A collector that listens for new blockchain event logs based on a [Event],
/// and generates a stream of events of type `E`.
pub struct EventCollector<P, E> {
    event: Event<P, E>,
}

impl<P, E> EventCollector<P, E> {
    pub fn new(event: Event<P, E>) -> Self {
        Self { event }
    }
}

/// Implementation of the [Collector](Collector) trait for the [EventCollector](EventCollector).
#[async_trait]
impl<P, E> Collector<E> for EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>> {
        let stream = self.event.watch().await?.into_stream();
        let stream = stream.filter_map(|el| el.map(|(e, _)| e).ok());
        Ok(Box::pin(stream))
    }
}
