//! Adapts a [`broadcast::Receiver`] into a cancellation-aware, lag-logging
//! [`Stream`].
//!
//! The engine fans events to strategies and actions to executors over
//! [`broadcast`] channels. Each consumer's loop has to handle the same three
//! concerns: a [`Lagged`](tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged)
//! receiver (log and skip), a closed channel (stop), and cooperative shutdown
//! (stop). [`into_stream`] folds all three into one `Stream` so a consumer can
//! just `while let Some(item) = stream.next().await { … }`.

use futures::StreamExt;
use tokio::sync::broadcast;
use tokio_stream::Stream;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Wraps `receiver` as a `Stream` that logs and skips lag, ends when the
/// channel closes, and ends when `cancel` fires. `label` tags the log lines.
pub(crate) fn into_stream<T>(
    receiver: broadcast::Receiver<T>,
    cancel: CancellationToken,
    label: &'static str,
) -> impl Stream<Item = T>
where
    T: Clone + Send + 'static,
{
    BroadcastStream::new(receiver)
        .filter_map(move |result| async move {
            match result {
                Ok(item) => Some(item),
                Err(BroadcastStreamRecvError::Lagged(n)) => {
                    warn!("{label} receiver lagged, skipped {n} messages");
                    None
                }
            }
        })
        .take_until(async move {
            cancel.cancelled().await;
            info!("{label} shutting down");
        })
}

#[cfg(test)]
mod test {
    use super::*;
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn yields_sent_items() {
        let (tx, rx) = broadcast::channel::<u32>(16);
        let mut stream = Box::pin(into_stream(rx, CancellationToken::new(), "test"));

        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, Some(2));
    }

    #[tokio::test]
    async fn skips_lag_and_keeps_flowing() {
        // Capacity 2, four sends without consuming: the receiver lags by 2.
        let (tx, rx) = broadcast::channel::<u32>(2);
        let mut stream = Box::pin(into_stream(rx, CancellationToken::new(), "test"));
        for i in 1..=4 {
            tx.send(i).unwrap();
        }

        // The lag is swallowed (not surfaced as an item or an end); the
        // surviving items still flow through.
        assert_eq!(stream.next().await, Some(3));
        assert_eq!(stream.next().await, Some(4));
    }

    #[tokio::test]
    async fn ends_when_channel_closes() {
        let (tx, rx) = broadcast::channel::<u32>(16);
        let mut stream = Box::pin(into_stream(rx, CancellationToken::new(), "test"));

        tx.send(1).unwrap();
        drop(tx);

        assert_eq!(stream.next().await, Some(1));
        assert_eq!(stream.next().await, None);
    }

    #[tokio::test]
    async fn ends_when_cancelled_even_with_an_open_channel() {
        let (tx, rx) = broadcast::channel::<u32>(16);
        let cancel = CancellationToken::new();
        let mut stream = Box::pin(into_stream(rx, cancel.clone(), "test"));

        tx.send(1).unwrap();
        assert_eq!(stream.next().await, Some(1));

        cancel.cancel();
        assert_eq!(stream.next().await, None, "cancellation ends the stream");

        drop(tx); // keep the channel alive until after the assertion
    }
}
