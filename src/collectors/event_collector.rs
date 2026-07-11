use crate::collectors::fallback::subscribe_or_poll;
use crate::persistence::{BlockPosition, Indexed, PersistableCollector};
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

/// The live-stream item for one decoded log, or `None` to skip it.
///
/// A log re-sent with `removed: true` is a reorg *retraction*: the node is
/// telling us the event no longer happened. Delivering it as a fresh event
/// would hand strategies a second occurrence — and persist a duplicate row
/// that replays forever after — so it becomes an [`Indexed::Retract`] the
/// persistence window uses to drop the orphaned buffered rows (the only signal
/// a same-height reorg gives). A log with no block number cannot be indexed.
fn live_item<E>(event: E, log: &alloy::rpc::types::Log) -> Option<Indexed<BlockPosition, E>> {
    let Some(block) = log.block_number else {
        tracing::warn!(
            removed = log.removed,
            "Event log missing block number; skipping"
        );
        return None;
    };
    if log.removed {
        tracing::warn!(block, "reorged (removed) event log: retracting");
        return Some(Indexed::Retract(BlockPosition(block)));
    }
    Some(Indexed::Event(BlockPosition(block), event))
}

/// The `(block, event)` for one decoded *historical* log, or `None` to skip
/// it. A `query_range` snapshot never carries retractions — a removed log
/// there is simply not part of the canonical range — so it is dropped.
fn indexed_event<E>(event: E, log: &alloy::rpc::types::Log) -> Option<(u64, E)> {
    match live_item(event, log)? {
        Indexed::Event(position, event) => Some((position.0, event)),
        Indexed::Retract(_) => None,
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

    /// The decoded live stream behind both `subscribe` and
    /// `subscribe_indexed`: positioned events plus reorg retractions (via
    /// [`live_item`]). The single site that drops decode failures; the live
    /// `subscribe` keeps only events, persistence consumes retractions too.
    async fn indexed_stream(&self) -> Result<CollectorStream<'_, Indexed<BlockPosition, E>>> {
        let stream = self.raw_stream().await?;
        let stream = stream.filter_map(|el| match el {
            Ok((event, log)) => live_item(event, &log),
            Err(e) => {
                tracing::warn!("Failed to decode event log: {}", e);
                None
            }
        });
        Ok(Box::pin(stream))
    }
}

#[async_trait]
impl<P, E> Collector<E> for EventCollector<P, E>
where
    P: Provider,
    E: SolEvent + Send + Sync,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, E>> {
        // Retractions cannot be delivered to strategies (an event cannot be
        // un-happened downstream); only fresh events flow.
        let stream = self.indexed_stream().await?.filter_map(|item| match item {
            Indexed::Event(_, event) => Some(event),
            Indexed::Retract(_) => None,
        });
        Ok(Box::pin(stream))
    }
}

/// The [`EventCollector`] is block-aware: it recovers each event's block number
/// from its [`Log`](alloy::rpc::types::Log) and can replay a historical range
/// via the provider, so it can be wrapped with persistence. Its [`Position`] is
/// the built-in [`BlockPosition`], so existing block code needs no custom
/// position type; `u64` block numbers are wrapped/unwrapped at this alloy
/// boundary.
///
/// [`Position`]: crate::persistence::Position
#[async_trait]
impl<P, E> PersistableCollector<E> for EventCollector<P, E>
where
    P: Provider + Clone + Send + Sync,
    E: SolEvent + Send + Sync,
{
    type Pos = BlockPosition;

    async fn subscribe_indexed(&self) -> Result<CollectorStream<'_, Indexed<BlockPosition, E>>> {
        self.indexed_stream().await
    }

    async fn query_range(
        &self,
        from: BlockPosition,
        to: BlockPosition,
    ) -> Result<CollectorStream<'_, (BlockPosition, E)>> {
        // Reuse the collector's filter (address + signature), narrowed to the
        // requested block range, against a clone of the provider.
        let ranged = Event::new(self.event.provider.clone(), self.event.filter.clone())
            .from_block(from.0)
            .to_block(to.0);
        let events: Vec<(BlockPosition, E)> = ranged
            .query()
            .await?
            .into_iter()
            .filter_map(|(event, log)| indexed_event(event, &log))
            .map(|(block, event)| (BlockPosition(block), event))
            .collect();
        Ok(Box::pin(tokio_stream::iter(events)))
    }

    async fn tip(&self) -> Result<BlockPosition> {
        Ok(BlockPosition(self.event.provider.get_block_number().await?))
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

    /// A `removed: true` log becomes an [`Indexed::Retract`] on the live path
    /// (see [`live_item`]); one with no block number cannot be positioned and
    /// is skipped.
    #[test]
    fn removed_logs_become_retractions_on_the_live_path() {
        assert_eq!(
            live_item((), &log_at(Some(5), true)),
            Some(Indexed::Retract(BlockPosition(5)))
        );
        assert_eq!(live_item((), &log_at(None, true)), None);
    }

    /// A live log carries its block number through; one with no block number
    /// cannot be indexed and is skipped.
    #[test]
    fn live_logs_carry_their_block_number() {
        assert_eq!(
            live_item((), &log_at(Some(5), false)),
            Some(Indexed::Event(BlockPosition(5), ()))
        );
        assert_eq!(live_item((), &log_at(None, false)), None);
    }

    /// A historical `query_range` snapshot never carries retractions: a
    /// removed log there is simply not part of the canonical range.
    #[test]
    fn historical_removed_logs_are_dropped() {
        assert_eq!(indexed_event((), &log_at(Some(5), true)), None);
        assert_eq!(indexed_event((), &log_at(Some(5), false)), Some((5, ())));
    }
}
