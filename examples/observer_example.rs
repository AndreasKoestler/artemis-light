//! Watching a pipeline with a passive `Observer` — no Anvil or external node
//! required.
//!
//! An observer is one more subscriber on the engine's event and action
//! channels: it sees everything strategies and executors see while producing
//! and perturbing nothing. There is deliberately no error channel through
//! which observing could fail the pipeline — use it for metrics, logging, or
//! shadow analysis.
//!
//! Run with:
//! ```sh
//! cargo run --example observer_example
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;
use artemis_light::{
    engine::Engine,
    types::{ActionStream, Collector, CollectorStream, Executor, Observer, Strategy},
};
use async_trait::async_trait;

const TICKS: u64 = 6;

/// A collector that emits `0..TICKS` on a short interval.
struct TickCollector;

#[async_trait]
impl Collector<u64> for TickCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u64>> {
        let stream = futures::stream::unfold(0u64, |n| async move {
            if n >= TICKS {
                return None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            Some((n, n + 1))
        });
        Ok(Box::pin(stream))
    }
}

/// A strategy that acts only on even ticks — so the observer's event and
/// action counts differ, showing it watches both channels independently.
struct EvenTickStrategy;

#[async_trait]
impl Strategy<u64, u64> for EvenTickStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: u64) -> Result<ActionStream<'_, u64>> {
        let actions = if event.is_multiple_of(2) {
            vec![event]
        } else {
            vec![]
        };
        Ok(Box::pin(futures::stream::iter(actions)))
    }
}

/// An executor that just prints — it has no idea it is being observed.
struct PrintExecutor;

#[async_trait]
impl Executor<u64> for PrintExecutor {
    async fn execute(&mut self, action: u64) -> Result<()> {
        println!("[executor] acting on tick {action}");
        Ok(())
    }
}

/// A telemetry observer: counts every event and every action crossing the
/// channels, and signals `done` once it has seen the whole run. Neither
/// method returns a `Result` — observation cannot fail the pipeline.
struct Telemetry {
    events: Arc<AtomicU64>,
    actions: Arc<AtomicU64>,
    expected_actions: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Telemetry {
    fn check_done(&mut self) {
        if self.events.load(Ordering::SeqCst) == TICKS
            && self.actions.load(Ordering::SeqCst) == self.expected_actions
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
    }
}

#[async_trait]
impl Observer<u64, u64> for Telemetry {
    async fn observe_event(&mut self, event: u64) {
        println!("[observer] event {event}");
        self.events.fetch_add(1, Ordering::SeqCst);
        self.check_done();
    }

    async fn observe_action(&mut self, action: u64) {
        println!("[observer] action {action}");
        self.actions.fetch_add(1, Ordering::SeqCst);
        self.check_done();
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let events = Arc::new(AtomicU64::new(0));
    let actions = Arc::new(AtomicU64::new(0));
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut engine = Engine::<u64, u64>::default();
    engine.add_collector(Box::new(TickCollector));
    engine.add_strategy(Box::new(EvenTickStrategy));
    engine.add_executor(Box::new(PrintExecutor));
    engine.add_observer(Box::new(Telemetry {
        events: events.clone(),
        actions: actions.clone(),
        expected_actions: TICKS.div_ceil(2),
        done: Some(done_tx),
    }));

    println!("Starting engine — the observer watches both channels...\n");
    let mut handle = engine.run().await?;

    // Run until the observer has seen the whole pipeline, then shut down
    // cooperatively.
    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!(
        "\nTelemetry: {} events fanned to strategies, {} actions fanned to executors",
        events.load(Ordering::SeqCst),
        actions.load(Ordering::SeqCst),
    );
    Ok(())
}
