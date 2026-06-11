//! A miniature liquidation bot assembled from the reliability wrappers and
//! risk guards — the combinators of `reliability_example`, composed the way a
//! production bot would:
//!
//! - the strategy's risk policy lives *outside* the strategy, visible at
//!   composition time: `filter_actions` (minimum profit, maximum notional,
//!   collateral allowlist) and `cooldown` (one shot per period);
//! - the submission stack reads as a sentence: `retry` the private relay,
//!   `fallback` to the public mempool, `rate_limit` to the provider cap,
//!   `circuit_breaker` to fail closed, `gated` behind the kill switch;
//! - the operator keeps the artifacts the engine never sees: the breaker
//!   handle and the kill switch;
//! - per-route policies under one umbrella `Action`: bundles are retried,
//!   alerts are only rate-limited;
//! - a candidate next-gen relay shadows live traffic in `dry_run` — paper
//!   trading in production.
//!
//! No external node required. Run with:
//! ```sh
//! cargo run --example liquidation_bot_example
//! ```

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    engine::Engine,
    executor_ext::{ExecutorExt, RetryPolicy},
    strategy_ext::StrategyExt,
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;

/// The risk policy, in one place: the numbers the risk gate enforces.
const MIN_PROFIT_USD: f64 = 50.0;
const MAX_NOTIONAL_USD: f64 = 250_000.0;
const ALLOWED_COLLATERAL: [&str; 2] = ["HYPE", "UBTC"];

/// A delinquent borrower the strategy wants to liquidate.
#[derive(Clone, Debug)]
struct Liquidation {
    borrower: &'static str,
    collateral: &'static str,
    expected_profit_usd: f64,
    notional_usd: f64,
}

/// The engine-wide umbrella action: each route gets its own reliability
/// policy on the executor side.
#[derive(Clone, Debug)]
enum Action {
    SubmitLiquidation(Liquidation),
    Alert(String),
}

/// Emits `count` sequential block numbers on a fixed interval.
struct BlockTicker {
    interval: Duration,
    count: u64,
}

#[async_trait]
impl Collector<u64> for BlockTicker {
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

/// The scripted order book: which delinquent borrower each block reveals,
/// chosen so every guard in the composition gets exercised once.
fn candidate_for(block: u64) -> Option<Liquidation> {
    match block {
        // Dust: the risk gate drops it (profit below minimum).
        0 => Some(Liquidation {
            borrower: "0xd00d",
            collateral: "HYPE",
            expected_profit_usd: 8.0,
            notional_usd: 12_000.0,
        }),
        // Clean: fires. The relay times out once; `retry` absorbs it.
        1 => Some(Liquidation {
            borrower: "0xbeef",
            collateral: "HYPE",
            expected_profit_usd: 120.0,
            notional_usd: 80_000.0,
        }),
        // Arrives 250ms after 0xbeef fired: the 600ms cooldown swallows it.
        2 => Some(Liquidation {
            borrower: "0xcafe",
            collateral: "UBTC",
            expected_profit_usd: 95.0,
            notional_usd: 40_000.0,
        }),
        // Over the notional cap: the gate drops it, the review route alerts.
        4 => Some(Liquidation {
            borrower: "0xbabe",
            collateral: "UBTC",
            expected_profit_usd: 200.0,
            notional_usd: 500_000.0,
        }),
        // Collateral not allowlisted: the gate drops it.
        5 => Some(Liquidation {
            borrower: "0xf00d",
            collateral: "MEME",
            expected_profit_usd: 75.0,
            notional_usd: 9_000.0,
        }),
        // Clean, past the cooldown: fires. The relay rejects this borrower
        // outright; retries exhaust and `fallback` broadcasts it instead.
        6 => Some(Liquidation {
            borrower: "0xface",
            collateral: "HYPE",
            expected_profit_usd: 60.0,
            notional_usd: 30_000.0,
        }),
        _ => None,
    }
}

/// Finds one candidate liquidation per block. Deliberately free of risk
/// checks — the policy is attached outside, where it is visible.
struct LiquidationStrategy;

#[async_trait]
impl Strategy<u64, Liquidation> for LiquidationStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, block: u64) -> Result<ActionStream<'_, Liquidation>> {
        let candidates = match candidate_for(block) {
            Some(liq) => {
                println!(
                    "[strategy] block {block}: candidate {} (+${:.0}, ${:.0}k {})",
                    liq.borrower,
                    liq.expected_profit_usd,
                    liq.notional_usd / 1_000.0,
                    liq.collateral
                );
                vec![liq]
            }
            None => vec![],
        };
        Ok(Box::pin(futures::stream::iter(candidates)))
    }
}

/// Routes over-cap candidates to a human instead of silently losing them:
/// profitable positions the risk gate refuses deserve a manual review.
struct ReviewQueueStrategy;

#[async_trait]
impl Strategy<u64, String> for ReviewQueueStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, block: u64) -> Result<ActionStream<'_, String>> {
        let alerts = candidate_for(block)
            .filter(|liq| liq.notional_usd > MAX_NOTIONAL_USD)
            .map(|liq| {
                format!(
                    "manual review: {} is profitable (+${:.0}) but ${:.0}k notional exceeds the cap",
                    liq.borrower,
                    liq.expected_profit_usd,
                    liq.notional_usd / 1_000.0
                )
            });
        Ok(Box::pin(futures::stream::iter(alerts)))
    }
}

/// Fired once the expected number of bundles has landed, by whichever
/// executor lands them.
#[derive(Clone)]
struct DoneSignal {
    remaining: Arc<AtomicU32>,
    tx: Arc<Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
}

impl DoneSignal {
    fn new(expected: u32) -> (Self, tokio::sync::oneshot::Receiver<()>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (
            Self {
                remaining: Arc::new(AtomicU32::new(expected)),
                tx: Arc::new(Mutex::new(Some(tx))),
            },
            rx,
        )
    }

    fn submitted(&self) {
        if self.remaining.fetch_sub(1, Ordering::SeqCst) == 1
            && let Some(tx) = self.tx.lock().unwrap().take()
        {
            let _ = tx.send(());
        }
    }
}

/// The private relay, with two faults for the wrappers to handle: every
/// bundle's first attempt times out (transient — `retry` absorbs it), and one
/// borrower's bundle is rejected on every attempt (`fallback` reroutes it).
struct PrivateRelayExecutor {
    attempted: HashSet<&'static str>,
    rejects: &'static str,
    done: DoneSignal,
}

#[async_trait]
impl Executor<Liquidation> for PrivateRelayExecutor {
    async fn execute(&mut self, liq: Liquidation) -> Result<()> {
        if liq.borrower == self.rejects {
            anyhow::bail!("private relay rejected the bundle for {}", liq.borrower)
        }
        if self.attempted.insert(liq.borrower) {
            anyhow::bail!("private relay timed out for {} (transient)", liq.borrower)
        }
        println!(
            "[relay]    landed bundle for {} (second attempt)",
            liq.borrower
        );
        self.done.submitted();
        Ok(())
    }
}

/// The public-mempool backup: always accepts.
struct PublicMempoolExecutor {
    done: DoneSignal,
}

#[async_trait]
impl Executor<Liquidation> for PublicMempoolExecutor {
    async fn execute(&mut self, liq: Liquidation) -> Result<()> {
        println!(
            "[mempool]  broadcast bundle for {} via fallback",
            liq.borrower
        );
        self.done.submitted();
        Ok(())
    }
}

/// The next-gen relay under evaluation. It shadows live traffic in
/// `dry_run`, so this counter proves no bundle ever reaches it for real.
struct NextGenRelayExecutor {
    real_submissions: Arc<AtomicU32>,
}

#[async_trait]
impl Executor<Liquidation> for NextGenRelayExecutor {
    async fn execute(&mut self, liq: Liquidation) -> Result<()> {
        println!("[relay-v2] submitted bundle for {} FOR REAL", liq.borrower);
        self.real_submissions.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// The alerting sink: posts to a channel a human watches.
struct DiscordAlerter;

#[async_trait]
impl Executor<String> for DiscordAlerter {
    async fn execute(&mut self, message: String) -> Result<()> {
        println!("[alert]    {message}");
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    println!("── liquidation bot: every combinator in its production seat ──\n");

    // Two bundles are expected to land: 0xbeef on the relay (after one
    // retry) and 0xface on the mempool (after the relay rejects it).
    let (done, done_rx) = DoneSignal::new(2);
    let kill_switch = Arc::new(AtomicBool::new(true));
    let v2_submissions = Arc::new(AtomicU32::new(0));

    let mut engine = Engine::<u64, Action>::default();

    engine.add_collector(Box::new(BlockTicker {
        interval: Duration::from_millis(250),
        count: 7,
    }));

    // The risk policy, attached where the engine is wired — not buried in
    // the strategy. `cooldown` wraps `filter_actions`, so only an action
    // that survives the risk gate starts the clock.
    engine.add_strategy(Box::new(
        LiquidationStrategy
            .filter_actions(|liq: &Liquidation| {
                let rejection = if liq.expected_profit_usd < MIN_PROFIT_USD {
                    Some("profit below minimum")
                } else if liq.notional_usd > MAX_NOTIONAL_USD {
                    Some("notional over cap")
                } else if !ALLOWED_COLLATERAL.contains(&liq.collateral) {
                    Some("collateral not allowlisted")
                } else {
                    None
                };
                if let Some(reason) = rejection {
                    println!("[risk]     dropped {}: {reason}", liq.borrower);
                }
                rejection.is_none()
            })
            .cooldown(Duration::from_millis(600))
            .map_action(Action::SubmitLiquidation),
    ));
    engine.add_strategy(Box::new(ReviewQueueStrategy.map_action(Action::Alert)));

    // The submission stack, innermost to outermost: retry the relay, fall
    // back to the mempool, respect the provider cap, fail closed after
    // repeated failures, and let the kill switch veto everything. The
    // breaker handle is taken BEFORE the engine consumes the executor —
    // the handle is what the operator keeps.
    let relay_stack = PrivateRelayExecutor {
        attempted: HashSet::new(),
        rejects: "0xface",
        done: done.clone(),
    }
    .retry(RetryPolicy {
        max_retries: 1,
        base_delay: Duration::from_millis(50),
    })
    .fallback(PublicMempoolExecutor { done })
    .rate_limit(2)
    .circuit_breaker(3);
    let breaker = relay_stack.handle();

    engine.add_executor(Box::new(
        relay_stack
            .gated(Arc::clone(&kill_switch))
            .filter_map_action(|a: Action| match a {
                Action::SubmitLiquidation(liq) => Some(liq),
                _ => None,
            }),
    ));

    // The candidate relay shadows the same bundle route in dry-run: every
    // bundle is logged and dropped, none submitted.
    engine.add_executor(Box::new(
        NextGenRelayExecutor {
            real_submissions: Arc::clone(&v2_submissions),
        }
        .dry_run()
        .filter_map_action(|a: Action| match a {
            Action::SubmitLiquidation(liq) => Some(liq),
            _ => None,
        }),
    ));

    // Alerts get their own, lighter policy: rate-limited, never retried.
    engine.add_executor(Box::new(DiscordAlerter.rate_limit(1).filter_map_action(
        |a: Action| match a {
            Action::Alert(message) => Some(message),
            _ => None,
        },
    )));

    let mut handle = engine.run().await?;
    let _ = done_rx.await;

    // The operator's side: the engine consumed the executors, but the
    // breaker handle and the kill switch stayed out here.
    println!("\n[operator] circuit breaker open: {}", breaker.is_open());
    kill_switch.store(false, Ordering::SeqCst);
    println!("[operator] kill switch engaged — further bundles would be logged and dropped");

    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    println!(
        "[operator] relay v2 made {} real submissions while shadowing live traffic in dry run",
        v2_submissions.load(Ordering::SeqCst)
    );

    println!("\nDone!");
    Ok(())
}
