use crate::persistence::PersistableCollector;
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
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        let stream = self.event.subscribe().await?.into_stream();
        let stream = stream.filter_map(|el| match el {
            Ok((e, _)) => Some(e),
            Err(e) => {
                tracing::warn!("Failed to decode event log: {}", e);
                None
            }
        });
        Ok(Box::pin(stream))
    }
}

/// The [`EventCollector`] is block-aware: it recovers each event's block number
/// from its [`Log`](alloy::rpc::types::Log) and can replay a historical range
/// via the provider, so it can be wrapped with persistence.
#[async_trait]
impl<P, E> PersistableCollector<E> for EventCollector<P, E>
where
    P: Provider + Clone + Send + Sync,
    E: SolEvent + Send + Sync,
{
    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, (u64, E)>> {
        let stream = self.event.subscribe().await?.into_stream();
        let stream = stream.filter_map(|el| match el {
            Ok((event, log)) => match log.block_number {
                Some(block) => Some((block, event)),
                None => {
                    tracing::warn!("Event log missing block number; skipping");
                    None
                }
            },
            Err(e) => {
                tracing::warn!("Failed to decode event log: {}", e);
                None
            }
        });
        Ok(Box::pin(stream))
    }

    async fn query_range(&self, from: u64, to: u64) -> Result<CollectorStream<'_, (u64, E)>> {
        // Reuse the collector's filter (address + signature), narrowed to the
        // requested block range, against a clone of the provider.
        let ranged = Event::new(self.event.provider.clone(), self.event.filter.clone())
            .from_block(from)
            .to_block(to);
        let events: Vec<(u64, E)> = ranged
            .query()
            .await?
            .into_iter()
            .filter_map(|(event, log)| log.block_number.map(|block| (block, event)))
            .collect();
        Ok(Box::pin(tokio_stream::iter(events)))
    }

    async fn tip(&self) -> Result<u64> {
        Ok(self.event.provider.get_block_number().await?)
    }
}
