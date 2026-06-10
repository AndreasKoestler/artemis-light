//! A self-contained example wiring a collector, strategy, and executor
//! through the Artemis Engine — no Anvil or external node required.
//!
//! Run with:
//! ```sh
//! cargo run --example basic_example
//! ```

use anyhow::Result;
use artemis_light::{
    collector_ext::CollectorExt,
    engine::Engine,
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;

/// A collector that emits sequential `u64` ticks on a fixed interval.
struct TickCollector {
    interval: std::time::Duration,
    count: u64,
}

impl TickCollector {
    fn new(interval: std::time::Duration, count: u64) -> Self {
        Self { interval, count }
    }
}

#[async_trait]
impl Collector<u64> for TickCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, u64>> {
        let interval = self.interval;
        let count = self.count;
        let stream = futures::stream::unfold(0u64, move |n| async move {
            if n >= count {
                return None;
            }
            tokio::time::sleep(interval).await;
            println!("[collector] tick {n}");
            Some((n, n + 1))
        });
        Ok(Box::pin(stream))
    }
}

/// A strategy that doubles every incoming event value.
struct DoubleStrategy;

#[async_trait]
impl Strategy<u64, u64> for DoubleStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: u64) -> Result<ActionStream<'_, u64>> {
        let doubled = event * 2;
        println!("[strategy] {event} -> {doubled}");
        Ok(Box::pin(futures::stream::iter(vec![doubled])))
    }
}

/// An executor that prints each action it receives and signals `done` once it
/// has handled the expected number of actions.
struct PrintExecutor {
    remaining: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl Executor<u64> for PrintExecutor {
    async fn execute(&mut self, action: u64) -> Result<()> {
        println!("[executor] action = {action}");
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining == 0
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const TICKS: u64 = 5;

    // Build a collector and use `.map()` to add 100 to every tick.
    let collector =
        TickCollector::new(std::time::Duration::from_millis(500), TICKS).map(|tick| tick + 100);

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut engine = Engine::<u64, u64>::default();
    engine.add_collector(Box::new(collector));
    engine.add_strategy(Box::new(DoubleStrategy));
    engine.add_executor(Box::new(PrintExecutor {
        remaining: TICKS,
        done: Some(done_tx),
    }));

    println!("Starting engine — will process {TICKS} ticks then exit...\n");
    let mut handle = engine.run().await?;

    // Run until the executor reports the last action handled, then shut down
    // cooperatively: cancel the token and wait for every task to exit. A real
    // binary would select between this and Ctrl-C / `handle.fatal` (see the
    // README's minimal example).
    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!("\nDone!");
    Ok(())
}
