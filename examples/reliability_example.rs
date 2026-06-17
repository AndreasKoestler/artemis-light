//! The reliability layer for executors and the risk guards for strategies:
//! `ExecutorExt::retry`, `fallback`, `rate_limit`, `circuit_breaker`,
//! `deadline`, and `gated`/`dry_run` wrap a submission sink the way
//! `reconnect` guards a collector; `StrategyExt::filter_actions` and
//! `cooldown` keep the risk policy visible at composition time. No external
//! node required.
//!
//! Run with:
//! ```sh
//! cargo run --example reliability_example
//! ```

use std::collections::HashSet;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Result;
use artemis_light::{
    engine::Engine,
    executor_ext::{ExecutorExt, Expires, RetryPolicy},
    strategy_ext::StrategyExt,
    types::{ActionStream, Collector, CollectorStream, Executor, Strategy},
};
use async_trait::async_trait;
use tokio::time::Instant;

/// A candidate trade with its expected profit, in basis points.
#[derive(Clone, Debug)]
struct Trade {
    id: u64,
    profit_bps: i64,
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

/// Profit per tick: some quotes lose money, some collide with the cooldown.
const PROFITS_BPS: [i64; 6] = [-5, 12, 8, 30, 2, 25];

/// Quotes one candidate [`Trade`] per tick — profitable or not. The risk
/// policy deliberately does NOT live here: it is attached outside, with
/// `filter_actions` and `cooldown`, where it is visible at composition time.
struct QuoteStrategy;

#[async_trait]
impl Strategy<u64, Trade> for QuoteStrategy {
    async fn sync_state(&mut self) -> Result<()> {
        Ok(())
    }

    async fn process_event(&mut self, tick: u64) -> Result<ActionStream<'_, Trade>> {
        let trade = Trade {
            id: tick,
            profit_bps: PROFITS_BPS[(tick as usize) % PROFITS_BPS.len()],
        };
        println!("[quote]    trade {} at {} bps", trade.id, trade.profit_bps);
        Ok(Box::pin(futures::stream::iter(vec![trade])))
    }
}

/// Fired once the expected number of trades has been submitted, by whichever
/// executor submits them.
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

/// A primary RPC with two faults: every trade's *first* attempt fails
/// transiently (the `retry` wrapper absorbs it), and one trade id fails
/// permanently (the `fallback` wrapper reroutes it).
struct PrimaryRpc {
    seen: HashSet<u64>,
    dead_id: u64,
    done: DoneSignal,
}

#[async_trait]
impl Executor<Trade> for PrimaryRpc {
    async fn execute(&mut self, trade: Trade) -> Result<()> {
        if trade.id == self.dead_id {
            anyhow::bail!("primary RPC rejects trade {} every time", trade.id)
        }
        if self.seen.insert(trade.id) {
            anyhow::bail!("primary RPC transient failure for trade {}", trade.id)
        }
        println!("[primary]  submitted trade {} (second attempt)", trade.id);
        self.done.submitted();
        Ok(())
    }
}

/// The public-mempool backup: always accepts.
struct PublicMempool {
    done: DoneSignal,
}

#[async_trait]
impl Executor<Trade> for PublicMempool {
    async fn execute(&mut self, trade: Trade) -> Result<()> {
        println!("[mempool]  submitted trade {} via fallback", trade.id);
        self.done.submitted();
        Ok(())
    }
}

/// An RPC that is simply down, for tripping the circuit breaker.
struct DeadRpc;

#[async_trait]
impl Executor<Trade> for DeadRpc {
    async fn execute(&mut self, _trade: Trade) -> Result<()> {
        anyhow::bail!("connection refused")
    }
}

/// A sink that prints what it submits, for the kill-switch and rate-limit
/// scenes.
struct PrintingRpc;

#[async_trait]
impl Executor<Trade> for PrintingRpc {
    async fn execute(&mut self, trade: Trade) -> Result<()> {
        println!("[printing] submitted trade {}", trade.id);
        Ok(())
    }
}

/// A trade stamped with the freshness window it was priced against.
#[derive(Clone, Debug)]
struct DatedTrade {
    id: u64,
    expires_at: Instant,
}

impl Expires for DatedTrade {
    fn expires_at(&self) -> Instant {
        self.expires_at
    }
}

/// A sink that prints what it submits, for the deadline scene.
struct DatedRpc;

#[async_trait]
impl Executor<DatedTrade> for DatedRpc {
    async fn execute(&mut self, trade: DatedTrade) -> Result<()> {
        println!("[dated]    submitted trade {}", trade.id);
        Ok(())
    }
}

fn trade(id: u64) -> Trade {
    Trade { id, profit_bps: 10 }
}

#[tokio::main]
async fn main() -> Result<()> {
    // ── Scene 1: an engine run with the full stack ─────────────────────────
    // Quotes at -5, 12, 8, 30, 2, 25 bps. The risk gate drops -5 and 2; the
    // cooldown swallows 8 (the strategy fired 300ms earlier on 12). Trades
    // 1 and 5 land on the primary after one retry each; trade 3 — which the
    // primary permanently rejects — falls back to the public mempool.
    println!("── engine run: risk-gated strategy, reliability-wrapped executor ──\n");

    let (done, done_rx) = DoneSignal::new(3);
    let kill_switch = Arc::new(AtomicBool::new(true));

    let mut engine = Engine::<u64, Trade>::default();
    engine.add_collector(Box::new(TickCollector {
        interval: Duration::from_millis(200),
        count: 6,
    }));
    engine.add_strategy(Box::new(
        QuoteStrategy
            .filter_actions(|t: &Trade| t.profit_bps >= 5)
            .cooldown(Duration::from_millis(300)),
    ));
    engine.add_executor(Box::new(
        PrimaryRpc {
            seen: HashSet::new(),
            dead_id: 3,
            done: done.clone(),
        }
        .retry(RetryPolicy {
            max_retries: 1,
            base_delay: Duration::from_millis(50),
        })
        .fallback(PublicMempool { done })
        .rate_limit(const { NonZeroU32::new(5).unwrap() })
        .circuit_breaker(const { NonZeroU32::new(3).unwrap() })
        .gated(Arc::clone(&kill_switch)),
    ));

    let mut handle = engine.run().await?;
    let _ = done_rx.await;
    handle.token.cancel();
    while handle.tasks.join_next().await.is_some() {}

    // ── Scene 2: the kill switch and paper trading ─────────────────────────
    println!("\n── kill switch and dry run ──\n");
    let flag = Arc::new(AtomicBool::new(true));
    let mut gated = PrintingRpc.gated(Arc::clone(&flag));
    gated.execute(trade(100)).await?;
    flag.store(false, Ordering::SeqCst);
    gated.execute(trade(101)).await?;
    println!("trade 101 was dropped by the kill switch (logged, Ok)");

    let mut paper = PrintingRpc.dry_run();
    paper.execute(trade(102)).await?;
    println!("trade 102 was paper-traded: logged, never submitted");

    // ── Scene 3: the circuit breaker fails closed ──────────────────────────
    println!("\n── circuit breaker ──\n");
    let breaker = DeadRpc.circuit_breaker(const { NonZeroU32::new(2).unwrap() });
    let operator = breaker.handle();
    let mut breaker = breaker;
    for id in 200..203 {
        let err = breaker.execute(trade(id)).await.unwrap_err();
        println!("trade {id}: {err}");
    }
    println!("circuit open: {}", operator.is_open());
    operator.reset();
    println!("after reset:  {}", operator.is_open());

    // ── Scene 4: the rate limit applies backpressure ───────────────────────
    println!("\n── rate limit ──\n");
    let mut limited = PrintingRpc.rate_limit(const { NonZeroU32::new(2).unwrap() });
    let start = Instant::now();
    for id in 300..304 {
        limited.execute(trade(id)).await?;
    }
    println!(
        "4 submissions through rate_limit(2) took {:?} — the cap waits, it never drops",
        start.elapsed()
    );

    // ── Scene 5: the deadline drops stale actions ──────────────────────────
    // The strategy stamps the freshness window when it prices the trade; the
    // wrapper checks it at submission. Trade 401's window has already passed
    // (the check is `now >= expires_at`), so it is dropped — logged, Ok —
    // exactly like a gated-off action.
    println!("\n── deadline ──\n");
    let mut dated = DatedRpc.deadline();
    let now = Instant::now();
    dated
        .execute(DatedTrade {
            id: 400,
            expires_at: now + Duration::from_secs(1),
        })
        .await?;
    dated
        .execute(DatedTrade {
            id: 401,
            expires_at: now,
        })
        .await?;
    println!("trade 401 expired before submission: dropped (logged, Ok)");

    println!("\nDone!");
    Ok(())
}
