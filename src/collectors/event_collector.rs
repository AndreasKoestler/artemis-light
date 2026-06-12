use crate::collectors::fallback::subscribe_or_poll;
use crate::persistence::PersistableCollector;
use crate::types::{Collector, CollectorStream};
use alloy::{contract::Event, providers::Provider, rpc::types::Log, sol_types::SolEvent};
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

/// The `(block, event)` to deliver for one decoded log, or `None` to skip it.
///
/// A log re-sent with `removed: true` is a reorg *retraction*: the node is
/// telling us the event no longer happened. Delivering it as a fresh event
/// would hand strategies a second occurrence — and persist a duplicate row
/// that replays forever after. A log with no block number cannot be indexed.
fn indexed_event<E>(event: E, log: &alloy::rpc::types::Log) -> Option<(u64, E)> {
    if log.removed {
        tracing::warn!(
            block = log.block_number,
            "skipping reorged (removed) event log"
        );
        return None;
    }
    match log.block_number {
        Some(block) => Some((block, event)),
        None => {
            tracing::warn!("Event log missing block number; skipping");
            None
        }
    }
}

/// The raw decoded `(event, log)` stream, before reorg/index filtering.
/// Subscription and poller deliberately share this item type.
type RawEventStream<'a, E> = CollectorStream<'a, alloy::sol_types::Result<(E, Log)>>;

impl<P, E> EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    /// The `(event, log)` source shared by `subscribe` and
    /// `subscribe_indexed`: pubsub when available, filter polling otherwise.
    async fn raw_stream(&self) -> Result<RawEventStream<'_, E>> {
        subscribe_or_poll(
            "contract events",
            self.subscription_stream(),
            self.polling_stream(),
        )
        .await
    }

    /// Decoded events over pubsub. Fails on transports without pubsub.
    async fn subscription_stream(&self) -> Result<RawEventStream<'_, E>> {
        Ok(Box::pin(self.event.subscribe().await?.into_stream()))
    }

    /// Decoded events via a polled log filter.
    async fn polling_stream(&self) -> Result<RawEventStream<'_, E>> {
        Ok(Box::pin(self.event.watch().await?.into_stream()))
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
        let stream = self.raw_stream().await?;
        let stream = stream.filter_map(|el| match el {
            Ok((e, log)) => indexed_event(e, &log).map(|(_, e)| e),
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
        let stream = self.raw_stream().await?;
        let stream = stream.filter_map(|el| match el {
            Ok((event, log)) => indexed_event(event, &log),
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
            .filter_map(|(event, log)| indexed_event(event, &log))
            .collect();
        Ok(Box::pin(tokio_stream::iter(events)))
    }

    async fn tip(&self) -> Result<u64> {
        Ok(self.event.provider.get_block_number().await?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::rpc::types::Log;

    fn log_at(block: Option<u64>, removed: bool) -> Log {
        Log {
            block_number: block,
            removed,
            ..Default::default()
        }
    }

    /// On a reorg, nodes re-send previously delivered logs with
    /// `removed: true` to signal *retraction*. Treating one as a fresh event
    /// would hand strategies a second occurrence of something that no longer
    /// happened — and persist a duplicate row that replays forever after.
    #[test]
    fn removed_logs_are_retractions_not_events() {
        assert_eq!(indexed_event((), &log_at(Some(5), true)), None);
    }

    /// A live log carries its block number through; one with no block number
    /// cannot be indexed and is skipped.
    #[test]
    fn live_logs_carry_their_block_number() {
        assert_eq!(indexed_event((), &log_at(Some(5), false)), Some((5, ())));
        assert_eq!(indexed_event((), &log_at(None, false)), None);
    }
}
