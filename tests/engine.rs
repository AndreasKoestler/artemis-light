//! Engine startup-ordering tests: every strategy must already be draining the
//! event channel before any collector task starts, so a subscribe-time burst
//! larger than the bounded broadcast ring — a Persisted collector's
//! replay-once segment is exactly that — is not lost to ring overflow while a
//! strategy is still syncing.

use anyhow::Result;
use artemis_light::engine::Engine;
use artemis_light::types::{ActionStream, Collector, CollectorStream, Strategy};
use async_trait::async_trait;
use std::time::Duration;
use tokio::sync::mpsc;

/// Emits `0..count` as soon as it is subscribed, yielding between events so a
/// concurrently draining consumer keeps pace — a stand-in for a Persisted
/// collector's replay, which floods stored history exactly once.
struct ReplayBurstCollector {
    count: u32,
}

#[async_trait]
impl Collector<u32> for ReplayBurstCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u32>> {
        let count = self.count;
        Ok(Box::pin(async_stream::stream! {
            for i in 0..count {
                yield i;
                tokio::task::yield_now().await;
            }
        }))
    }
}

/// Records every event it sees; its sync deliberately dawdles.
struct SlowSyncStrategy {
    sync_delay: Duration,
    seen: mpsc::UnboundedSender<u32>,
}

#[async_trait]
impl Strategy<u32, u32> for SlowSyncStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        tokio::time::sleep(self.sync_delay).await;
        Ok(())
    }

    async fn process_event(&mut self, event: u32) -> Result<ActionStream<'_, u32>> {
        let _ = self.seen.send(event);
        Ok(Box::pin(futures::stream::empty()))
    }
}

/// A subscribe-time burst far larger than the event ring, paired with a
/// strategy whose sync is slow: no event may be lost. Under a
/// collectors-before-sync startup the burst floods the capacity-4 ring while
/// the strategy is still syncing, and everything but the ring's tail is gone
/// before the strategy task exists to drain it.
#[tokio::test]
async fn replay_burst_survives_slow_strategy_sync() {
    let (tx, mut rx) = mpsc::unbounded_channel();

    let mut engine = Engine::<u32, u32>::default().with_event_channel_capacity(4);
    engine.add_collector(Box::new(ReplayBurstCollector { count: 64 }));
    engine.add_strategy(Box::new(SlowSyncStrategy {
        sync_delay: Duration::from_millis(100),
        seen: tx,
    }));

    let mut handle = engine.run().await.unwrap();

    let mut seen = Vec::new();
    while seen.len() < 64 {
        match tokio::time::timeout(Duration::from_secs(2), rx.recv()).await {
            Ok(Some(event)) => seen.push(event),
            _ => break,
        }
    }

    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    assert_eq!(
        seen,
        (0..64).collect::<Vec<_>>(),
        "a replay-sized burst must not be lost to ring overflow during strategy sync"
    );
}
