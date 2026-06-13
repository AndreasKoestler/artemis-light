use crate::types::{Collector, CollectorStream};
use anyhow::Result;
use async_trait::async_trait;
use tokio::sync::broadcast::Sender;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;

/// A [`Collector`] over an in-process [`broadcast`](tokio::sync::broadcast)
/// channel: each event sent to the channel becomes a collected event. It holds
/// the `Sender` (not a `Receiver`) so every `subscribe` mints a fresh receiver
/// — surviving the reconnect driver's re-subscription, where a single
/// `Receiver` could not. The seam through which execution feedback (an
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
