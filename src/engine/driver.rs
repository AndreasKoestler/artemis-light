//! The [Collector Driver]: the loop that runs one
//! [`Collector`](crate::types::Collector)'s full lifecycle.
//!
//! The driver subscribes to the collector's event stream, pumps its events into
//! the engine's broadcast channel, and — on a lost or failed stream — consults
//! the collector's [`ReconnectPolicy`] to decide whether to sleep for a
//! [`Retry`](Decision::Retry) or, on a [`Fatal`](Decision::Fatal) verdict,
//! cancel the fatal token and then the root token (tearing down every task).
//!
//! The policy refuses all I/O and keeps no clock; the driver supplies it: the
//! actual `subscribe`, the actual `sleep`, the actual `send`. The engine spawns
//! one driver per collector and otherwise stays out of reconnection.

use tokio::sync::broadcast;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::types::Collector;

use super::reconnect::{Decision, ReconnectConfig, ReconnectPolicy};

/// The cancellation tokens a [`run`] driver observes and owns.
pub(crate) struct CollectorTokens {
    /// The driver's own shutdown signal (a child of `root`). Observed while
    /// sleeping for a retry and while pumping the stream.
    pub child: CancellationToken,
    /// Observe-only fatal cause. Cancelled — before `root` — when the policy
    /// declares the collector unrecoverable, so the binary can tell a fatal
    /// shutdown apart from a caller-initiated one.
    pub fatal: CancellationToken,
    /// The root token shared by every task. Cancelled after `fatal` on a fatal
    /// escalation, tearing the whole engine down.
    pub root: CancellationToken,
}

/// Runs one collector's full lifecycle until it is cancelled or escalates to
/// [`Fatal`](super::reconnect::Decision::Fatal). See the [module docs](self).
pub(crate) async fn run<E>(
    collector: Box<dyn Collector<E>>,
    config: ReconnectConfig,
    events: broadcast::Sender<E>,
    tokens: CollectorTokens,
) where
    E: Clone + Send + 'static,
{
    info!("starting collector...");
    let CollectorTokens { child, fatal, root } = tokens;

    // The Fatal escalation: cancel the observe-only fatal cause first, then the
    // root token that tears down every task. Kept in one place so the two-step
    // ordering can't drift between the creation-failure and stream-end sites.
    let escalate = || {
        fatal.cancel();
        root.cancel();
    };

    let mut policy = ReconnectPolicy::new(config);
    loop {
        let mut stream = match collector.subscribe().await {
            Ok(s) => s,
            Err(e) => {
                error!("collector stream creation failed: {e}");
                // fall through to the creation-failure policy below.
                match policy.on_creation_failed() {
                    Decision::Retry { after } => {
                        warn!("retrying stream creation in {after:?}");
                        tokio::select! {
                            _ = child.cancelled() => return,
                            _ = tokio::time::sleep(after) => continue,
                        }
                    }
                    Decision::Fatal => {
                        error!("collector unrecoverable (creation), shutting down engine");
                        escalate();
                        return;
                    }
                }
            }
        };

        // The stream is established. How long it stays open before ending tells
        // the policy whether the end is a healthy provider-side recycle (reset)
        // or a flap that should march toward Fatal.
        let stream_open = tokio::time::Instant::now();

        loop {
            tokio::select! {
                _ = child.cancelled() => {
                    info!("collector shutting down");
                    return;
                }
                event = stream.next() => {
                    match event {
                        Some(event) => {
                            policy.on_events_received();
                            if let Err(e) = events.send(event) {
                                error!("error sending event: {e}");
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        // Stream ended (e.g. the WebSocket dropped).
        match policy.on_stream_ended(stream_open.elapsed()) {
            Decision::Retry { after } => {
                warn!("collector stream ended, retrying in {after:?}");
                tokio::select! {
                    _ = child.cancelled() => return,
                    _ = tokio::time::sleep(after) => {}
                }
            }
            Decision::Fatal => {
                error!("collector unrecoverable (stream ended), shutting down engine");
                escalate();
                return;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::types::CollectorStream;
    use anyhow::Result;
    use async_trait::async_trait;
    use std::time::Duration;

    /// A collector that emits a fixed list of u32 events, then ends its stream.
    struct FixedThenEnd {
        items: Vec<u32>,
    }

    #[async_trait]
    impl Collector<u32> for FixedThenEnd {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
            Ok(Box::pin(futures::stream::iter(self.items.clone())))
        }
    }

    /// Builds a fresh `child`-of-`root` token set, returning handles to `fatal`
    /// and `root` so a test can observe escalation and drive shutdown.
    fn tokens() -> (CollectorTokens, CancellationToken, CancellationToken) {
        let root = CancellationToken::new();
        let child = root.child_token();
        let fatal = CancellationToken::new();
        (
            CollectorTokens {
                child,
                fatal: fatal.clone(),
                root: root.clone(),
            },
            fatal,
            root,
        )
    }

    fn config(max_failures: u32) -> ReconnectConfig {
        ReconnectConfig {
            max_failures,
            base_delay: Duration::from_millis(1),
            // The test collectors below all end their streams immediately, so any
            // positive threshold keeps those ends counting as failures.
            healthy_uptime: Duration::from_secs(30),
        }
    }

    /// A collector whose stream always ends immediately (never delivers).
    struct EndingCollector;

    #[async_trait]
    impl Collector<u32> for EndingCollector {
        async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
            Ok(Box::pin(futures::stream::empty::<u32>()))
        }
    }

    #[tokio::test]
    async fn pumps_events_into_the_sender() {
        let (tx, mut rx) = broadcast::channel::<u32>(16);
        let (toks, _fatal, root) = tokens();
        let collector = Box::new(FixedThenEnd {
            items: vec![1, 2, 3],
        });

        let handle = tokio::spawn(run(collector, config(100), tx, toks));

        assert_eq!(rx.recv().await.unwrap(), 1);
        assert_eq!(rx.recv().await.unwrap(), 2);
        assert_eq!(rx.recv().await.unwrap(), 3);

        root.cancel();
        handle.await.unwrap();
    }

    /// A collector that never delivers must march to Fatal after the threshold
    /// of consecutive stream-ends, cancelling `fatal` *and* `root`.
    #[tokio::test(start_paused = true)]
    async fn empty_stream_escalates_to_fatal_and_cancels_both_tokens() {
        let (tx, _rx) = broadcast::channel::<u32>(16);
        let (toks, fatal, root) = tokens();

        run(Box::new(EndingCollector), config(2), tx, toks).await;

        assert!(fatal.is_cancelled(), "fatal cause should be cancelled");
        assert!(root.is_cancelled(), "root token should be cancelled");
    }

    /// A collector that delivers an event every reconnect resets the failure
    /// counter each cycle, so it must *never* escalate — it flaps indefinitely.
    #[tokio::test(start_paused = true)]
    async fn delivered_events_prevent_escalation() {
        let (tx, mut rx) = broadcast::channel::<u32>(16);
        let (toks, fatal, root) = tokens();
        // Threshold of 2, but one event per stream keeps the counter pinned at 1.
        let collector = Box::new(FixedThenEnd { items: vec![7] });

        let handle = tokio::spawn(run(collector, config(2), tx, toks));

        // Three events means three full reconnect cycles survived without Fatal.
        for _ in 0..3 {
            assert_eq!(rx.recv().await.unwrap(), 7);
        }
        assert!(!fatal.is_cancelled(), "delivering events must not escalate");

        root.cancel();
        handle.await.unwrap();
    }

    /// Cancelling the driver's own token shuts it down cleanly — it returns
    /// without ever touching the fatal cause.
    #[tokio::test(start_paused = true)]
    async fn child_cancellation_shuts_down_without_fatal() {
        let (tx, _rx) = broadcast::channel::<u32>(16);
        let (toks, fatal, root) = tokens();
        let collector = Box::new(EndingCollector); // would retry forever otherwise

        let handle = tokio::spawn(run(collector, config(1_000), tx, toks));
        // Let it enter its retry/backoff loop, then ask it to stop.
        tokio::task::yield_now().await;
        root.cancel();

        handle.await.unwrap();
        assert!(
            !fatal.is_cancelled(),
            "a caller-initiated shutdown is not a fatal one"
        );
    }

    /// Persistent stream-*creation* failure escalates to Fatal just like a
    /// persistent stream-*end* — both feed the one counter.
    #[tokio::test(start_paused = true)]
    async fn creation_failure_escalates_to_fatal() {
        /// A collector whose `subscribe` always fails.
        struct FailingCollector;

        #[async_trait]
        impl Collector<u32> for FailingCollector {
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
                Err(anyhow::anyhow!("creation failed"))
            }
        }

        let (tx, _rx) = broadcast::channel::<u32>(16);
        let (toks, fatal, root) = tokens();

        run(Box::new(FailingCollector), config(2), tx, toks).await;

        assert!(fatal.is_cancelled(), "creation failure should escalate");
        assert!(root.is_cancelled());
    }

    /// The driver sleeps for exactly the policy's backoff curve before
    /// escalating: with `base_delay` 1s and threshold 3, it retries after 2s
    /// then 4s, so 6s of virtual time elapse before Fatal.
    #[tokio::test(start_paused = true)]
    async fn honours_the_backoff_curve_before_escalating() {
        let (tx, _rx) = broadcast::channel::<u32>(16);
        let (toks, _fatal, _root) = tokens();
        let config = ReconnectConfig {
            max_failures: 3,
            base_delay: Duration::from_secs(1),
            // EndingCollector ends at ~0s uptime, well under this, so every end
            // counts toward Fatal and the backoff curve is exercised in full.
            healthy_uptime: Duration::from_secs(30),
        };

        let start = tokio::time::Instant::now();
        run(Box::new(EndingCollector), config, tx, toks).await;

        assert_eq!(start.elapsed(), Duration::from_secs(6));
    }

    /// A quiet-but-healthy subscription: the stream stays open well past
    /// `healthy_uptime`, delivers nothing, then is dropped — repeatedly, as a
    /// load-balanced RPC endpoint recycling a long-lived connection. Each end is
    /// a healthy recycle, so the counter resets every cycle and the collector
    /// must reconnect forever without ever escalating to Fatal.
    #[tokio::test(start_paused = true)]
    async fn healthy_long_lived_drops_never_escalate() {
        /// A collector whose stream stays open for `uptime` (delivering nothing)
        /// then ends.
        struct QuietThenDrop {
            uptime: Duration,
        }

        #[async_trait]
        impl Collector<u32> for QuietThenDrop {
            async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
                let uptime = self.uptime;
                // Yield no items; sleep `uptime` of virtual time, then end.
                Ok(Box::pin(futures::stream::unfold(
                    (),
                    move |()| async move {
                        tokio::time::sleep(uptime).await;
                        None::<(u32, ())>
                    },
                )))
            }
        }

        let (tx, _rx) = broadcast::channel::<u32>(16);
        let (toks, fatal, root) = tokens();
        // Threshold of 2 would escalate after two flaps; the 60s uptime is past
        // the 30s `healthy_uptime`, so it never does.
        let collector = Box::new(QuietThenDrop {
            uptime: Duration::from_secs(60),
        });

        let handle = tokio::spawn(run(collector, config(2), tx, toks));
        // Let many reconnect cycles elapse (each ~60s open + ~1ms backoff).
        tokio::time::sleep(Duration::from_secs(60 * 20)).await;
        assert!(
            !fatal.is_cancelled(),
            "a healthy long-lived stream the provider keeps recycling must not escalate"
        );
        assert!(!root.is_cancelled());

        root.cancel();
        handle.await.unwrap();
    }
}
