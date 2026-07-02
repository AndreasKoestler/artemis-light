//! Bring-your-own PostgreSQL pool: persisting through a caller-owned `PgPool`.
//!
//! A service that already runs one shared, tuned `sqlx::PgPool` can hand that
//! pool to artemis-light instead of making it open a second one from a URL. The
//! store *borrows* the pool: it reads and writes through it but never closes or
//! reconfigures it. This example demonstrates the whole story:
//!
//!   1. build a caller-owned multi-connection `PgPool` (the store never caps or
//!      polices the connection count — inject-pool.STORE.5),
//!   2. construct a `PostgresStore` from a clone of that pool with
//!      [`PostgresStore::with_pool`] and persist 3 events,
//!   3. rebuild a *second* store from the **same** pool to show resume/replay,
//!   4. drop every store handle and prove the injected pool is still usable.
//!
//! Unlike `persistence_example` (in-memory SQLite), this needs a real
//! PostgreSQL. It reads `DATABASE_URL` from the environment and does **not**
//! provision a database itself. The quickest local Postgres is one line of
//! Docker (see `examples/README.md`):
//!
//! ```sh
//! docker run --rm -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16
//! ```
//!
//! Then, with `anvil` on `$PATH` (ships with Foundry) and the `postgres`
//! feature enabled:
//!
//! ```sh
//! DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo run --example injected_pool_example --features postgres
//! ```

use std::sync::Arc;

use alloy::node_bindings::Anvil;
use alloy::primitives::U256;
use alloy::providers::{ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::Result;
use artemis_light::collectors::EventCollector;
use artemis_light::persistence::{PersistExt, PostgresStore, Store};
use artemis_light::types::Collector;
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;

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
    // The example reads its database from the environment; it never provisions
    // one itself (see the module docs for a one-line Docker Postgres). Library
    // code performs no environment reads — this lookup lives in the example only.
    let database_url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "Error: DATABASE_URL must be set to a PostgreSQL URL, e.g. postgres://postgres:postgres@localhost:5432/postgres"
            );
            std::process::exit(1);
        }
    };

    // ---- The caller owns and configures the pool. ----
    //
    // A multi-connection pool (max_connections(5)): `with_pool` accepts any
    // connection count and never caps or overrides it (inject-pool.STORE.5). An
    // unreachable database fails *here*, on the caller's own connect — not inside
    // artemis-light.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    // Reset the example's own tables so re-runs are deterministic (it targets a
    // throwaway demo database). This drops through the caller's pool.
    sqlx::query(r#"DROP TABLE IF EXISTS "value_set""#)
        .execute(&pool)
        .await?;
    sqlx::query(r#"DROP TABLE IF EXISTS "_artemis_progress""#)
        .execute(&pool)
        .await?;

    // A local chain mining one block per second, plus a deployed emitter.
    let anvil = Anvil::new().block_time(1).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = Arc::new(ProviderBuilder::new().wallet(signer).connect_ws(ws).await?);
    let contract = Emitter::deploy(provider.clone()).await?;

    // ---- First run: persist 3 events through the injected pool. ----
    //
    // Build the store from a *clone* of the caller's pool (a clone is another
    // handle to the same shared pool), then wrap the collector with persistence.
    let store = Arc::new(PostgresStore::with_pool(pool.clone()));
    let persisted = EventCollector::new(contract.ValueSet_filter()).with_persistence(store.clone());

    println!("First run — persisting 3 events through the injected pool:");
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

    // Each event landed in its own block. A block is flushed once a higher one is
    // seen, so the resume point has advanced past the earliest events.
    match store.last_block("value_set").await? {
        Some(block) => println!("Highest persisted block: {block}"),
        None => println!("Highest persisted block: (none yet)"),
    }

    // ---- "Restart": a second store over the SAME injected pool. ----
    //
    // A new `Persisted` over a store rebuilt from the same pool is exactly what a
    // relaunched process does: it replays the stored history from the database
    // reached through the borrowed pool, then backfills the still-open block.
    println!("Restart — a new store over the same injected pool recovers history:");
    let recovered_store = Arc::new(PostgresStore::with_pool(pool.clone()));
    let recovered =
        EventCollector::new(contract.ValueSet_filter()).with_persistence(recovered_store.clone());
    let mut stream = recovered.subscribe().await?;
    for _ in 0..3 {
        let event = stream.next().await.expect("recovered event");
        println!("  [recovered] ValueSet({})", event.value);
    }

    // ---- Drop every store handle, then prove the pool is still open. ----
    //
    // The store never calls `.close()` on a borrowed pool (inject-pool.OWNERSHIP.1),
    // so once every store handle is gone the caller's pool is still fully usable.
    drop(stream);
    drop(recovered);
    drop(recovered_store);
    drop(store);

    sqlx::query("SELECT 1").execute(&pool).await?;
    println!("Store dropped — injected pool still usable: SELECT 1 succeeded");

    println!("Done!");
    Ok(())
}
