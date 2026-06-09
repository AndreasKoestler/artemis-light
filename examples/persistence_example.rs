//! Recording events to SQLite and replaying them on the next subscribe.
//!
//! Wrapping any block-aware collector with `.with_persistence(store)` makes it
//! record every event it sees, and on the *first* subscribe replay the stored
//! history before catching up to (and following) the chain tip. This is how a
//! strategy survives a restart without re-syncing from genesis.
//!
//! Requires `anvil` on `$PATH` (ships with Foundry). Run with:
//! ```sh
//! cargo run --example persistence_example
//! ```

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

sol! {
    #[sol(rpc, bytecode = "6080604052348015600e575f5ffd5b5060d980601a5f395ff3fe6080604052348015600e575f5ffd5b50600436106030575f3560e01c80633fa4f2451460345780635524107714604d575b5f5ffd5b603b5f5481565b60405190815260200160405180910390f35b605c6058366004608d565b605e565b005b5f81815560405182917f012c78e2b84325878b1bd9d250d772cfe5bda7722d795f45036fa5e1e6e303fc91a250565b5f60208284031215609c575f5ffd5b503591905056fea264697066735822122050fddb04e40945ebc7c51aef06d27a86c4aa98943b773d9ffdc789caf784441064736f6c634300081e0033")]
    contract Emitter {
        uint256 public value;

        // `Persisted` derives the table/columns from the event, so it must be
        // `Serialize` (to write) and `Deserialize` (to replay).
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
    // A local chain mining one block per second, plus a deployed emitter.
    let anvil = Anvil::new().block_time(1).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = Arc::new(ProviderBuilder::new().wallet(signer).connect_ws(ws).await?);
    let contract = Emitter::deploy(provider.clone()).await?;

    // An in-memory store; use `SqliteStore::connect("sqlite:events.db")` to
    // persist across process restarts.
    let store = Arc::new(SqliteStore::connect("sqlite::memory:").await?);

    // ---- First run: record events as they arrive on the live chain. ----
    //
    // Wrap the event collector with persistence — the whole feature is this
    // one `.with_persistence(...)` call.
    let persisted = EventCollector::new(contract.ValueSet_filter()).with_persistence(store.clone());

    println!("First run — emitting 3 events on the live chain:");
    let mut stream = persisted.subscribe().await?;
    for v in [10u64, 20, 30] {
        contract
            .setValue(U256::from(v))
            .send()
            .await?
            .watch()
            .await?;
        let event = stream.next().await.expect("event");
        println!("  [live] ValueSet({})", event.value);
    }
    // Drop the subscription and the wrapper, as a process shutdown would.
    drop(stream);
    drop(persisted);

    // Each event landed in its own block. A block is flushed once a higher one
    // is seen, so 10 and 20 are persisted while 30's block is still "open".
    println!(
        "\nHighest persisted block: {:?}",
        store.last_block("value_set").await?
    );

    // ---- "Restart": a fresh wrapper over the same store. ----
    //
    // Replay runs on a wrapper's *first* subscribe (a reconnect of the same
    // wrapper does not re-replay), so recovering history after a restart means
    // a new `Persisted` over the same database — exactly what a relaunched
    // process does.
    println!("\nRestart — a new collector over the same SQLite store recovers history:");
    let recovered = EventCollector::new(contract.ValueSet_filter()).with_persistence(store.clone());
    let mut stream = recovered.subscribe().await?;

    // 10 and 20 are replayed straight from the database; 30's still-open block
    // is re-fetched by the backfill query — all three return with no new chain
    // activity needed.
    for _ in 0..3 {
        let event = stream.next().await.expect("recovered event");
        println!("  [recovered] ValueSet({})", event.value);
    }

    println!("\nDone!");
    Ok(())
}
