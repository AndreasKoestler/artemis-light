//! The subscribe-or-poll downgrade shared by every collector.
//!
//! Pubsub subscriptions need a WebSocket (or IPC) transport; over plain HTTP
//! every `eth_subscribe` fails. Rather than letting the Reconnect Policy
//! retry a call that can never succeed, a collector hands this helper two
//! futures — its pubsub subscription and its filter-polling counterpart —
//! and the helper downgrades to polling, with a warning, when the
//! subscription cannot be established.

use crate::types::CollectorStream;
use anyhow::Result;
use std::future::Future;
use tracing::warn;

/// Await `subscribe`; on error, warn (naming `what` and the error) and await
/// `poll` instead. A poll failure propagates as the `subscribe()` error the
/// Reconnect Policy counts.
///
/// Stateless by design: every call — one per reconnect — re-attempts the
/// subscription first, so a recovered pubsub endpoint upgrades back
/// automatically. The cost is one failed RPC and one warning line per
/// reconnect on HTTP-only providers.
pub(crate) async fn subscribe_or_poll<'a, E>(
    what: &str,
    subscribe: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
    poll: impl Future<Output = Result<CollectorStream<'a, E>>> + Send,
) -> Result<CollectorStream<'a, E>> {
    match subscribe.await {
        Ok(stream) => Ok(stream),
        Err(e) => {
            warn!("Error subscribing to {what} ({e}); polling instead");
            poll.await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn stream_of(items: Vec<u32>) -> Result<CollectorStream<'static, u32>> {
        Ok(Box::pin(tokio_stream::iter(items)))
    }

    // A typed failing arm: `subscribe_or_poll` takes `impl Trait` arguments,
    // so its type parameters cannot be supplied via turbofish (E0632) and a
    // bare `async { Err(..) }` block would leave `E` uninferable.
    async fn failing(msg: &'static str) -> Result<CollectorStream<'static, u32>> {
        Err(anyhow::anyhow!(msg))
    }

    /// A working subscription must be used as-is; the poll arm must not even
    /// start, since building it costs RPC calls (filter creation).
    #[tokio::test]
    async fn working_subscription_skips_polling() {
        let polled = Arc::new(AtomicBool::new(false));
        let flag = polled.clone();
        let poll = async move {
            flag.store(true, Ordering::SeqCst);
            stream_of(vec![9])
        };

        let stream = subscribe_or_poll("test", async { stream_of(vec![1, 2]) }, poll)
            .await
            .unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![1, 2]);
        assert!(!polled.load(Ordering::SeqCst), "poll arm must not run");
    }

    /// A failed subscription (e.g. no pubsub on an HTTP transport) downgrades
    /// to the polling stream instead of erroring out.
    #[tokio::test]
    async fn failed_subscription_downgrades_to_polling() {
        let stream = subscribe_or_poll("test", failing("no pubsub"), async { stream_of(vec![7]) })
            .await
            .unwrap();
        assert_eq!(stream.collect::<Vec<_>>().await, vec![7]);
    }

    /// When polling fails too, its error must propagate out of `subscribe()`
    /// so the Reconnect Policy counts the failure and drives the retry.
    #[tokio::test]
    async fn poll_failure_propagates_to_reconnect_policy() {
        let result =
            subscribe_or_poll("test", failing("no pubsub"), failing("filters unsupported")).await;
        assert!(result.is_err());
        let err_msg = result.err().unwrap().to_string();
        assert!(err_msg.contains("filters unsupported"));
    }
}
