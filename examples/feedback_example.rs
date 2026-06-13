//! Execution feedback: a strategy reacts to its own submissions. The executor
//! publishes an `ExecutionOutcome` per action via `ExecutorExt::report`; a
//! `ChannelCollector` over the same broadcast channel feeds those verdicts
//! back as events, and the strategy stops re-submitting a trade once it sees a
//! successful outcome for it — closing the loop without a blind cooldown. No
//! external node required.
//!
//! Run with:
//! ```sh
//! cargo run --example feedback_example
//! ```

use std::collections::HashSet;
use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    collector_ext::CollectorExt,
    collectors::ChannelCollector,
    engine::Engine,
    executor_ext::{ExecutionOutcome, ExecutorExt},
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;
use tokio::sync::broadcast;

/// A trade the strategy wants submitted.
#[derive(Clone, Debug)]
struct Trade {
    id: u64,
}

/// The umbrella event type: clock ticks plus fed-back submission verdicts.
#[derive(Clone, Debug)]
enum Event {
    /// The tick sequence number is carried for realism but not read here.
    Tick(#[allow(dead_code)] u64),
    Outcome(ExecutionOutcome<Trade>),
}

/// Emits `count` sequential ticks on a fixed interval.
struct TickCollector {
    interval: Duration,
    count: u64,
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
            Some((n, n + 1))
        });
        Ok(Box::pin(stream))
    }
}

/// Re-submits trade id 1 on every tick until it sees a successful outcome for
/// it, then goes quiet — driven entirely by feedback.
struct PersistentStrategy {
    confirmed: HashSet<u64>,
}

#[async_trait]
impl Strategy<Event, Trade> for PersistentStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, event: Event) -> Result<ActionStream<'_, Trade>> {
        match event {
            Event::Tick(_) if !self.confirmed.contains(&1) => {
                println!("[strategy] trade 1 unconfirmed; submitting");
                Ok(Box::pin(futures::stream::iter(vec![Trade { id: 1 }])))
            }
            Event::Tick(_) => {
                println!("[strategy] trade 1 confirmed; nothing to do");
                Ok(Box::pin(futures::stream::empty()))
            }
            Event::Outcome(o) => {
                if o.result.is_ok() {
                    println!("[strategy] outcome: trade {} confirmed", o.action.id);
                    self.confirmed.insert(o.action.id);
                } else {
                    println!(
                        "[strategy] outcome: trade {} failed; will retry",
                        o.action.id
                    );
                }
                Ok(Box::pin(futures::stream::empty()))
            }
        }
    }
}

/// Fails the first submission of each id, succeeds thereafter.
struct FlakyExecutor {
    seen: HashSet<u64>,
}

#[async_trait]
impl Executor<Trade> for FlakyExecutor {
    async fn execute(&mut self, trade: Trade) -> Result<()> {
        if self.seen.insert(trade.id) {
            anyhow::bail!("first submission of trade {} fails", trade.id)
        }
        println!("[executor] submitted trade {}", trade.id);
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (outcomes, _) = broadcast::channel::<ExecutionOutcome<Trade>>(64);

    let mut engine = Engine::<Event, Trade>::default();

    // Ticks, widened into the umbrella event type.
    engine.add_collector(Box::new(
        TickCollector {
            interval: Duration::from_millis(200),
            count: 5,
        }
        .map(Event::Tick),
    ));

    // Feedback: verdicts re-enter as events.
    engine.add_collector(Box::new(
        ChannelCollector::new(outcomes.clone()).map(Event::Outcome),
    ));

    engine.add_strategy(Box::new(PersistentStrategy {
        confirmed: HashSet::new(),
    }));

    engine.add_executor(Box::new(
        FlakyExecutor {
            seen: HashSet::new(),
        }
        .report(outcomes),
    ));

    let mut handle = engine.run().await?;
    // Let the loop run a few ticks, then shut down cooperatively.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!("\nDone! Trade 1 failed once, then confirmed; the strategy went quiet.");
    Ok(())
}
