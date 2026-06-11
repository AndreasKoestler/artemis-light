//! Composing narrow strategies and executors into one engine with the
//! adapter combinators: collectors are widened into an umbrella `Event` enum
//! with `CollectorExt::map`, strategies are mounted with
//! `StrategyExt::filter_map_event` + `map_action`, and executors are routed
//! with `ExecutorExt::filter_map_action`. No external node required.
//!
//! Run with:
//! ```sh
//! cargo run --example adapters_example
//! ```

use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    collector_ext::CollectorExt,
    engine::Engine,
    executor_ext::ExecutorExt,
    strategy_ext::StrategyExt,
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;

/// The engine-wide umbrella event: every collector's output, widened.
#[derive(Clone, Debug)]
enum Event {
    Tick(u64),
    Price(f64),
}

/// The engine-wide umbrella action: every strategy's output, widened.
#[derive(Clone, Debug)]
enum Action {
    Submit(u64),
    Log(String),
}

/// Emits `count` sequential `u64` ticks on a fixed interval. Knows nothing
/// about the umbrella `Event` type.
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

/// Emits a fixed series of `f64` prices on an interval. Also narrow.
struct PriceCollector {
    interval: Duration,
    prices: Vec<f64>,
}

#[async_trait]
impl Collector<f64> for PriceCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, f64>> {
        let interval = self.interval;
        let prices = self.prices.clone();
        let stream = futures::stream::unfold(prices.into_iter(), move |mut it| async move {
            let price = it.next()?;
            tokio::time::sleep(interval).await;
            Some((price, it))
        });
        Ok(Box::pin(stream))
    }
}

/// A narrow strategy over `u64` ticks: submits every even tick. Written and
/// testable without any knowledge of `Event` or `Action`.
struct EvenTickStrategy;

#[async_trait]
impl Strategy<u64, u64> for EvenTickStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, tick: u64) -> Result<ActionStream<'_, u64>> {
        let actions = if tick.is_multiple_of(2) {
            vec![tick]
        } else {
            vec![]
        };
        Ok(Box::pin(futures::stream::iter(actions)))
    }
}

/// A narrow strategy over `f64` prices: logs an alert above a threshold.
struct PriceAlertStrategy {
    threshold: f64,
}

#[async_trait]
impl Strategy<f64, String> for PriceAlertStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, price: f64) -> Result<ActionStream<'_, String>> {
        let actions = if price > self.threshold {
            vec![format!("price {price} above threshold {}", self.threshold)]
        } else {
            vec![]
        };
        Ok(Box::pin(futures::stream::iter(actions)))
    }
}

/// A narrow executor for `u64` submissions; signals `done` after the expected
/// number of actions so the example can exit.
struct SubmitExecutor {
    remaining: u64,
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

#[async_trait]
impl Executor<u64> for SubmitExecutor {
    async fn execute(&mut self, tick: u64) -> Result<()> {
        println!("[submit] tick {tick}");
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining == 0
            && let Some(done) = self.done.take()
        {
            let _ = done.send(());
        }
        Ok(())
    }
}

/// A narrow executor for log lines.
struct LogExecutor;

#[async_trait]
impl Executor<String> for LogExecutor {
    async fn execute(&mut self, line: String) -> Result<()> {
        println!("[log] {line}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    const TICKS: u64 = 6;
    const EVEN_TICKS: u64 = 3; // ticks 0, 2, 4

    let (done_tx, done_rx) = tokio::sync::oneshot::channel();

    let mut engine = Engine::<Event, Action>::default();

    // Producer side: widen each narrow collector into the umbrella Event.
    engine.add_collector(Box::new(
        TickCollector {
            interval: Duration::from_millis(200),
            count: TICKS,
        }
        .map(Event::Tick),
    ));
    engine.add_collector(Box::new(
        PriceCollector {
            interval: Duration::from_millis(300),
            prices: vec![0.9, 1.4, 2.3],
        }
        .map(Event::Price),
    ));

    // Consumer side: project the umbrella Event down to each narrow
    // strategy, and lift each strategy's actions into the umbrella Action.
    engine.add_strategy(Box::new(
        EvenTickStrategy
            .filter_map_event(|e: Event| match e {
                Event::Tick(t) => Some(t),
                _ => None,
            })
            .map_action(Action::Submit),
    ));
    engine.add_strategy(Box::new(
        PriceAlertStrategy { threshold: 1.0 }
            .filter_map_event(|e: Event| match e {
                Event::Price(p) => Some(p),
                _ => None,
            })
            .map_action(Action::Log),
    ));

    // Route only matching umbrella Actions to each narrow executor.
    engine.add_executor(Box::new(
        SubmitExecutor {
            remaining: EVEN_TICKS,
            done: Some(done_tx),
        }
        .filter_map_action(|a: Action| match a {
            Action::Submit(t) => Some(t),
            _ => None,
        }),
    ));
    engine.add_executor(Box::new(LogExecutor.filter_map_action(
        |a: Action| match a {
            Action::Log(line) => Some(line),
            _ => None,
        },
    )));

    println!("Starting engine — two narrow strategies in one umbrella engine...\n");
    let mut handle = engine.run().await?;

    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!("\nDone!");
    Ok(())
}
