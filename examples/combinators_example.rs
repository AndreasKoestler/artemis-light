//! Composing collectors with the `CollectorExt` combinators — no Anvil or
//! external node required.
//!
//! Demonstrates:
//! - `map`: transform a collector's events into another type
//! - `filter_map`: drop and transform events in one step
//! - `merge`: interleave two live sources into one collector
//! - `chain`: deliver one source's events strictly before another's
//! - `fallback`: prefer a primary source, falling back to a backup if the
//!   primary's subscribe fails
//! - `merge_all` / `chain_all`: the same over a dynamic list of sources
//!
//! Run with:
//! ```sh
//! cargo run --example combinators_example
//! ```

use anyhow::Result;
use artemis_light::{
    collector_ext::{CollectorExt, chain_all, merge_all},
    types::{Collector, CollectorStream},
};
use async_trait::async_trait;
use futures::StreamExt;

/// A collector that replays a fixed list of items — a stand-in for any real
/// source (blocks, logs, pending txs, off-chain quotes, ...).
struct VecCollector<T> {
    items: Vec<T>,
}

impl<T> VecCollector<T> {
    fn new(items: Vec<T>) -> Self {
        Self { items }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> Collector<T> for VecCollector<T> {
    async fn subscribe(&self) -> Result<CollectorStream<'_, T>> {
        Ok(Box::pin(futures::stream::iter(self.items.clone())))
    }
}

/// A collector whose subscribe always fails — a stand-in for a primary
/// endpoint that is currently down.
struct DownCollector;

#[async_trait]
impl Collector<Event> for DownCollector {
    async fn subscribe(&self) -> Result<CollectorStream<'_, Event>> {
        anyhow::bail!("primary endpoint is down")
    }
}

/// The unified event type the strategy would consume. Combinators are how two
/// sources with different native shapes end up on one engine channel.
#[derive(Clone, Debug)]
enum Event {
    Block(u64),
    Quote { symbol: &'static str, cents: u64 },
}

#[tokio::main]
async fn main() -> Result<()> {
    // ---- `map`: a block-number feed becomes `Event::Block`s. -------------
    let blocks = VecCollector::new(vec![100u64, 101, 102]).map(Event::Block);

    // ---- `filter_map`: keep only quotes at or above a dollar, in one step.
    let quotes = VecCollector::new(vec![("ETH", 250_000u64), ("DUST", 3), ("BTC", 9_000_000)])
        .filter_map(|(symbol, cents)| (cents >= 100).then_some(Event::Quote { symbol, cents }));

    // ---- `merge`: both feeds behind a single `Collector<Event>`. ---------
    //
    // Events arrive in whichever order the sources produce them; the merged
    // composite is *one* collector to the engine (one driver, one reconnect
    // lifecycle). Register sources separately instead when each should
    // reconnect — and go fatal — independently.
    println!("merge: blocks and quotes interleaved as produced");
    let merged = blocks.merge(quotes);
    let mut stream = merged.subscribe().await?;
    while let Some(event) = stream.next().await {
        match event {
            Event::Block(number) => println!("  block #{number}"),
            Event::Quote { symbol, cents } => println!("  quote {symbol} @ {cents} cents"),
        }
    }

    // ---- `chain`: strict sequence, e.g. a historical batch before a live
    // feed. The second source subscribes eagerly (buffering at its source),
    // but its events are held back until the first stream ends.
    println!("\nchain: history drains fully before the live feed");
    let history = VecCollector::new(vec![1u64, 2, 3]);
    let live = VecCollector::new(vec![4u64, 5, 6]);
    let chained = history.chain(live);
    let mut stream = chained.subscribe().await?;
    while let Some(n) = stream.next().await {
        println!("  {n}");
    }

    // ---- `merge_all` / `chain_all`: the same combinators over a list built
    // at runtime, e.g. one source per configured market.
    println!("\nchain_all: a runtime-built list, delivered in registration order");
    let sources: Vec<Box<dyn Collector<u64>>> = vec![
        Box::new(VecCollector::new(vec![1u64, 2])),
        Box::new(VecCollector::new(vec![3u64])),
        Box::new(VecCollector::new(vec![4u64, 5])),
    ];
    let chained = chain_all(sources);
    let mut stream = chained.subscribe().await?;
    while let Some(n) = stream.next().await {
        println!("  {n}");
    }

    println!("\nmerge_all: the same list, interleaved as produced");
    let sources: Vec<Box<dyn Collector<u64>>> = vec![
        Box::new(VecCollector::new(vec![1u64, 2])),
        Box::new(VecCollector::new(vec![3u64])),
        Box::new(VecCollector::new(vec![4u64, 5])),
    ];
    let merged = merge_all(sources);
    let mut stream = merged.subscribe().await?;
    while let Some(n) = stream.next().await {
        println!("  {n}");
    }

    // ---- `fallback`: the primary endpoint is down, so the composite
    // transparently delivers the backup's events. A healthy primary would be
    // used instead, and the backup never subscribed.
    println!("\nfallback: primary is down, backup takes over");
    let resilient = DownCollector.fallback(VecCollector::new(vec![
        Event::Block(200),
        Event::Quote {
            symbol: "ETH",
            cents: 300_000,
        },
    ]));
    let events: Vec<Event> = resilient.subscribe().await?.collect().await;
    println!("  fallback delivered from backup: {events:?}");

    println!("\nDone!");
    Ok(())
}
