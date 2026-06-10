//! The collector reconnect lifecycle: retries with exponential backoff, the
//! failure counter resetting on delivery, and escalation to a fatal shutdown —
//! no Anvil or external node required.
//!
//! Every collector the engine spawns is run by a driver that, on a lost or
//! failed stream, consults a per-collector `ReconnectPolicy`: retry after a
//! backoff, or — after `max_failures` consecutive failures — declare the
//! collector fatal. A fatal verdict cancels the observe-only `handle.fatal`
//! token and then the root token; the library never calls `process::exit`,
//! the binary observes the token and decides.
//!
//! Run with:
//! ```sh
//! cargo run --example reconnect_example
//! ```

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use artemis_light::{
    engine::{Engine, reconnect::ReconnectConfig},
    types::{Collector, CollectorStream, Observer},
};
use async_trait::async_trait;
use futures::StreamExt;

/// A collector whose first `failures` subscribe attempts fail, then connects
/// and stays live — a WebSocket endpoint coming back after a brief outage.
struct FlakyCollector {
    failures: u32,
    attempts: AtomicU32,
    started: Instant,
}

impl FlakyCollector {
    fn new(failures: u32) -> Self {
        Self {
            failures,
            attempts: AtomicU32::new(0),
            started: Instant::now(),
        }
    }
}

#[async_trait]
impl Collector<u64> for FlakyCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u64>> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let elapsed = self.started.elapsed().as_millis();
        if attempt <= self.failures {
            println!("[collector] t+{elapsed:>4}ms attempt {attempt}: connection refused");
            anyhow::bail!("connection refused");
        }
        println!("[collector] t+{elapsed:>4}ms attempt {attempt}: connected");
        // Emit a few events, then stay pending like a live subscription. The
        // deliveries reset the policy's consecutive-failure counter.
        let stream = futures::stream::iter(vec![1u64, 2, 3]).chain(futures::stream::pending());
        Ok(Box::pin(stream))
    }
}

/// A collector that can never establish its stream.
struct BrokenCollector {
    attempts: AtomicU32,
    started: Instant,
}

impl BrokenCollector {
    fn new() -> Self {
        Self {
            attempts: AtomicU32::new(0),
            started: Instant::now(),
        }
    }
}

#[async_trait]
impl Collector<u64> for BrokenCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u64>> {
        let attempt = self.attempts.fetch_add(1, Ordering::SeqCst) + 1;
        let elapsed = self.started.elapsed().as_millis();
        println!("[collector] t+{elapsed:>4}ms attempt {attempt}: connection refused");
        anyhow::bail!("connection refused");
    }
}

/// Signals `done` after seeing `expected` events — how this example knows the
/// flaky collector recovered and delivered.
struct CountToDone {
    seen: u64,
    expected: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl Observer<u64, u64> for CountToDone {
    async fn observe_event(&mut self, event: u64) {
        println!("[observer] delivered event {event}");
        self.seen += 1;
        if self.seen == self.expected
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
    }
}

/// Retry quickly so the example runs in about a second; the delay before the
/// Nth retry is `base_delay * 2^N`.
const RECONNECT: ReconnectConfig = ReconnectConfig {
    max_failures: 3,
    base_delay: Duration::from_millis(100),
};

#[tokio::main]
async fn main() -> Result<()> {
    // ---- Phase 1: a transient outage is retried and recovered. -----------
    //
    // Two failed attempts stay under `max_failures: 3`, so the policy keeps
    // retrying (after 200ms, then 400ms) until the source connects. Delivery
    // resets the failure counter.
    println!("Phase 1 — transient outage: retry, back off, recover\n");

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let mut engine = Engine::<u64, u64>::default().with_reconnect_config(RECONNECT);
    engine.add_collector(Box::new(FlakyCollector::new(2)));
    engine.add_observer(Box::new(CountToDone {
        seen: 0,
        expected: 3,
        done: Some(done_tx),
    }));

    let mut handle = engine.run().await?;
    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    // ---- Phase 2: a permanent outage escalates to fatal. -----------------
    //
    // The third consecutive failure reaches `max_failures`, so the policy
    // declares the collector fatal: the engine cancels `handle.fatal` (the
    // reason) and then the root token (tearing down every task).
    println!("\nPhase 2 — permanent outage: exhaust retries, escalate to fatal\n");

    let mut engine = Engine::<u64, u64>::default().with_reconnect_config(RECONNECT);
    engine.add_collector(Box::new(BrokenCollector::new()));

    let mut handle = engine.run().await?;

    // A real binary selects between Ctrl-C and `handle.fatal` so it can tell
    // a caller-initiated shutdown apart from an unrecoverable collector.
    let fatal = tokio::select! {
        _ = tokio::signal::ctrl_c() => false,
        _ = handle.fatal.cancelled() => true,
    };
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    if fatal {
        println!("\nfatal token fired: collector unrecoverable, all tasks torn down.");
        println!("A real binary would `std::process::exit(1)` here so its");
        println!("orchestrator restarts the process with a fresh sync.");
    }
    Ok(())
}
