use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// A [`Collector`] over an in-process [`broadcast`](tokio::sync::broadcast)
/// channel: each event sent to the channel becomes a collected event. It holds
/// the `Sender` (not a `Receiver`) so every `subscribe` mints a fresh receiver
/// — a single `Receiver` could not be subscribed twice across the reconnect
/// driver's re-subscription. Delivery is best-effort during reconnect windows:
/// events sent between a stream's death and the re-subscribe are lost, since a
/// fresh receiver sees only later sends (with no live receiver the send itself
/// errors, which the sending side may drop quietly). The seam through which
/// execution feedback (an
/// [`ExecutionOutcome`](crate::executor_ext::ExecutionOutcome)) — or any
/// in-process source — re-enters the pipeline as events.
pub struct ChannelCollector<T> {
    sender: Sender<T>,
}

impl<T> ChannelCollector<T> {
    /// Creates a collector that emits every item sent to `sender`'s channel.
    pub fn new(sender: Sender<T>) -> Self {
        Self { sender }
    }
}

#[async_trait]
impl<T> Collector<T> for ChannelCollector<T>
where
    T: Clone + Send + Sync + 'static,
{
    async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
        let stream = BroadcastStream::new(self.sender.subscribe()).filter_map(|item| match item {
            Ok(item) => Some(item),
            Err(e) => {
                tracing::warn!("channel collector lagged: {e}");
                None
            }
        });
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast;

    #[tokio::test]
    async fn delivers_items_sent_after_subscribe() {
        let (tx, _rx) = broadcast::channel(8);
        let collector = ChannelCollector::new(tx.clone());
        let mut stream = collector.subscribe().await.unwrap();

        tx.send(1u32).unwrap();
        tx.send(2u32).unwrap();

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
    }

    /// A receiver that falls behind the channel's capacity yields a `Lagged`
    /// error; the collector swallows it (logging a warning) and keeps
    /// delivering the retained tail rather than surfacing the error or ending
    /// the stream.
    #[tokio::test]
    async fn a_lagged_receiver_skips_missed_items_without_erroring() {
        let (tx, _rx) = broadcast::channel(2);
        let collector = ChannelCollector::new(tx.clone());
        let mut stream = collector.subscribe().await.unwrap();

        // Overflow the capacity before the stream is polled: the oldest sends
        // are dropped and this receiver lags.
        for i in 0..5u32 {
            tx.send(i).unwrap();
        }

        // The lag surfaces as a filtered-out `None`, so the next delivered item
        // is the retained tail — the stream neither errors nor terminates.
        let next = stream.next().await;
        assert!(next.is_some(), "the stream survives a lag and yields the tail");
    }

    #[tokio::test]
    async fn a_second_subscribe_works_where_a_receiver_could_not() {
        let (tx, _rx) = broadcast::channel(8);
        let collector = ChannelCollector::new(tx.clone());

        // First subscription, then dropped — as a lost stream would be.
        let first = collector.subscribe().await.unwrap();
        drop(first);

        // The reconnect driver re-subscribes; the new stream sees later items.
        let mut second = collector.subscribe().await.unwrap();
        tx.send(42u32).unwrap();
        assert_eq!(second.next().await, Some(42));
    }
}
