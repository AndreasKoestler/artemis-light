//! Lagging the store behind the live edge to absorb reorgs.
//!
//! By default `.with_persistence(store)` writes a block as soon as the next one
//! arrives. That means a block that later reorgs out has *already* been written
//! — a stale row that replays forever after. `.with_confirmation_depth(n)`
//! instead holds the most recent `n` blocks in a confirmation buffer and writes
//! a block only once it is buried `n` blocks deep.
//!
//! That buffer is what makes reorgs safe: a reorg shallower than `n` is
//! corrected inside the buffer *before any row is written*, so the store only
//! ever finalizes the canonical chain (a reorg deeper than `n` halts
//! persistence, and a restart re-syncs). Critically, events still reach
//! strategies live and immediately — only the *store write* lags.
//!
//! This demo shows the observable consequence of that design: with a
//! confirmation depth of 3, five events are delivered live but only the two
//! that have aged past the depth are persisted; the three most recent stay
//! buffered until the chain buries them. A restart re-fetches that whole
//! window. The in-buffer reorg *correction* itself is proven deterministically
//! by the `confirmation_depth_corrects_a_shallow_reorg` integration test in
//! `tests/persistence.rs` (Anvil can't push a simulated reorg to a live log
//! subscription, so it can't exercise the correction here).
//!
//! Requires `anvil` on `$PATH` (ships with Foundry). Run with:
//! ```sh
//! cargo run --example confirmation_depth_example
//! ```

use std::num::NonZeroU64;
use std::sync::Arc;

use alloy::node_bindings::Anvil;
use alloy::primitives::U256;
use alloy::providers::{ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::Result;
use artemis_light::collectors::EventCollector;
use artemis_light::persistence::{PersistExt, SqliteStore, Store};
use artemis_light::types::Collector;
use futures::StreamExt;

// A compile-time `NonZeroU64`: the `.unwrap()` is evaluated by the compiler,
// so a zero would be a build error, never a runtime panic.
const CONFIRMATION_DEPTH: NonZeroU64 = NonZeroU64::new(3).unwrap();

sol! {
    #[sol(rpc, bytecode = "6080604052348015600e575f5ffd5b5060d980601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106030575f3560e01c80633fa4f2451460345780635524107714604d575b5f5ffd5b603b5f5481565b60405190815260200160405180910390f35b605c6058366004608d565b605e565b005b5f81815560405182917f012c78e2b84325878b1bd9d250d772cfe5bda7722d795f45036fa5e1e6e303fc91a250565b5f60208284031215609c575f5ffd5b503591905056fea264697066735822122050fddb04e40945ebc7c51aef06d27a86c4aa98943b773d9ffdc789caf784441064736f6c634300081e0033")]
    contract Emitter {
        uint256 public value;

        #[derive(serde::Serialize, serde::Deserialize, Debug)]
        event ValueSet(uint256 indexed value);

        function setValue(uint256 _value) external {
            value = _value;
            emit ValueSet(_value);
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Default Anvil mines exactly one block per transaction, so every `setValue`
    // below lands in its own block — the live edge advances one block per event.
    let anvil = Anvil::new().try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = Arc::new(ProviderBuilder::new().wallet(signer).connect_ws(ws).await?);
    let contract = Emitter::deploy(provider.clone()).await?;

    // An in-memory store; use `SqliteStore::connect("sqlite:events.db")` to
    // persist across process restarts.
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await?);

    // ---- First run: record events, but lag the store by the confirmation depth. ----
    //
    // The only difference from plain `.with_persistence(store)` is the
    // `.with_confirmation_depth(..)` knob: a block is written only once it is
    // that many blocks deep.
    let persisted = EventCollector::new(contract.ValueSet_filter())
        .with_persistence(store.clone())
        .with_confirmation_depth(CONFIRMATION_DEPTH);

    println!("First run — emitting 5 events on the live chain (one per block):");
    let mut stream = persisted.subscribe().await?;
    for v in [10u64, 20, 30, 40, 50] {
        contract
            .setValue(U256::from(v))
            .send()
            .await?
            .watch()
            .await?;
        // Every event is delivered to the strategy the instant it arrives —
        // the confirmation depth never delays delivery, only the store write.
        let event = stream.next().await.expect("event");
        println!("  [live] ValueSet({})", event.value);
    }
    // Drop the subscription and the wrapper, as a process shutdown would.
    drop(stream);
    drop(persisted);

    // All 5 events were delivered live, but only the blocks now buried at least
    // `CONFIRMATION_DEPTH` deep are written. The 5th event sits at the live edge,
    // so the most recent `CONFIRMATION_DEPTH` blocks (events 30, 40, 50) are
    // still buffered, unwritten — that buffer is the reorg cushion.
    println!(
        "\nConfirmation depth {CONFIRMATION_DEPTH}: highest persisted block is {:?} \
         — only events 10 and 20 are finalized; 30, 40, 50 are still buffered.",
        store.last_block("value_set").await?
    );

    // ---- "Restart": a fresh wrapper over the same store recovers everything. ----
    //
    // Replay returns the finalized rows (10, 20); the backfill re-fetches the
    // *whole* unflushed confirmation window from the chain (30, 40, 50) — not
    // just a single open block. A restart therefore never loses the buffered
    // tail, and on a real chain it re-fetches it from the canonical fork.
    println!("\nRestart — a new collector over the same store recovers all 5 events:");
    let recovered = EventCollector::new(contract.ValueSet_filter())
        .with_persistence(store.clone())
        .with_confirmation_depth(CONFIRMATION_DEPTH);
    let mut stream = recovered.subscribe().await?;
    for _ in 0..5 {
        let event = stream.next().await.expect("recovered event");
        println!("  [recovered] ValueSet({})", event.value);
    }

    println!("\nDone!");
    Ok(())
}
