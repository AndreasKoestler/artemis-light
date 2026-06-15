//! Serving indexed data over HTTP and navigating it with a tiny client.
//!
//! This example wires the whole story end to end:
//!
//!   1. a persistence pipeline records on-chain events into a **file-backed**
//!      SQLite database (`:memory:` is not servable — a second pool would see an
//!      empty database),
//!   2. a [`ServingLayer`] opens its own read-only pool over that same file and
//!      exposes the persisted tables as read-only HTTP/JSON, and
//!   3. a minimal HTTP client walks the API — health, indexing status, the table
//!      catalog, a table's schema, and paged / block-range-filtered rows.
//!
//! The client is a hand-rolled HTTP/1.1 `GET` over a raw TCP socket so the
//! example pulls in **no extra dependencies** — in real code reach for
//! `reqwest`/`hyper`. Responses are parsed with `serde_json` (already a dep).
//!
//! Requires the `serving` feature and `anvil` on `$PATH` (ships with Foundry):
//! ```sh
//! cargo run --example serving_example --features serving
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use alloy::node_bindings::Anvil;
use alloy::primitives::U256;
use alloy::providers::{Provider, ProviderBuilder, WsConnect};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::{Context, Result};
use artemis_light::ServingLayer;
use artemis_light::collectors::EventCollector;
use artemis_light::persistence::{PersistExt, SqliteStore, Store};
use artemis_light::types::Collector;
use futures::StreamExt;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

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
    // ---- 1. Index some events into a file-backed SQLite database. ----
    //
    // The serving layer reads the same file the writer commits to, so it must be
    // a real file, not `:memory:`. A tempdir keeps the example self-cleaning.
    let dir = tempfile::tempdir()?;
    let db_path = dir.path().join("events.db");
    let db_url = format!("sqlite:{}", db_path.display());

    let anvil = Anvil::new().block_time(1).try_spawn()?;
    let signer: PrivateKeySigner = anvil.keys()[0].clone().into();
    let ws = WsConnect::new(anvil.ws_endpoint());
    let provider = Arc::new(ProviderBuilder::new().wallet(signer).connect_ws(ws).await?);
    let contract = Emitter::deploy(provider.clone()).await?;

    let store = Arc::new(SqliteStore::connect(&db_url).await?);
    let persisted = EventCollector::new(contract.ValueSet_filter()).with_persistence(store.clone());

    println!("Indexing 5 ValueSet events into {db_url} ...");
    let mut stream = persisted.subscribe().await?;
    for v in [10u64, 20, 30, 40, 50] {
        contract
            .setValue(U256::from(v))
            .send()
            .await?
            .watch()
            .await?;
        let event = stream.next().await.expect("event");
        println!("  indexed ValueSet({})", event.value);
    }
    // A block is flushed to disk only once a higher block is seen, so the last
    // event's block is still "open". anvil keeps mining (block_time = 1), which
    // will close it shortly — the client polls /status below to wait for that.
    drop(stream);
    drop(persisted);

    // ---- 2. Stand up the read-only serving layer over that database. ----
    //
    // The one-liner in production is `ServingLayer::new(url, addr).serve(token)`,
    // which binds `addr` itself. Here we bind an ephemeral port ourselves (so the
    // example never clashes with a busy port) and drive the same router via the
    // public `into_router` seam — exactly what `serve` does internally.
    let shutdown = CancellationToken::new();
    let router = ServingLayer::new(&db_url, "127.0.0.1:0".parse::<SocketAddr>()?)
        .into_router()
        .await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = {
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await
        })
    };
    println!("\nServing read-only HTTP/JSON on http://{addr}\n");

    // ---- 3. Navigate the indexed data with a minimal HTTP client. ----

    // Liveness.
    get_json(addr, "/health").await?;

    // Indexing progress: poll until the writer has flushed all 5 events
    // (last_block stops moving once the final event's block is committed).
    println!("Waiting for the writer to flush every event ...");
    for attempt in 0.. {
        let status = get_value(addr, "/status").await?;
        let flushed = status["tables"]
            .as_array()
            .and_then(|t| t.iter().find(|t| t["table"] == "value_set"))
            .is_some();
        if flushed {
            print_labeled("GET /status", &status);
            break;
        }
        if attempt >= 20 {
            anyhow::bail!("writer did not flush value_set in time");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    // Table catalog (internal bookkeeping tables are hidden).
    get_json(addr, "/tables").await?;

    // The table's column schema (names + SQLite affinities).
    get_json(addr, "/tables/value_set/schema").await?;

    // First page of rows (ascending block order), capped to 2 per page.
    get_json(addr, "/tables/value_set/rows?limit=2").await?;

    // Second page via offset — keyset/offset paging walks the whole table.
    get_json(addr, "/tables/value_set/rows?limit=2&offset=2").await?;

    // Block-range filter: only rows whose block_number is in [from, to].
    let head = provider.get_block_number().await?;
    get_json(
        addr,
        &format!("/tables/value_set/rows?from_block=0&to_block={head}&limit=100"),
    )
    .await?;

    // An unknown table is a clean 404, not a crash.
    get_json(addr, "/tables/does_not_exist/schema").await?;

    // ---- Drain the server and exit. ----
    println!("Shutting down the serving layer ...");
    shutdown.cancel();
    server
        .await?
        .context("serving layer terminated with an error")?;

    // The writer's view agrees with what the API served.
    println!(
        "\nWriter-side highest persisted block for `value_set`: {:?}",
        store.last_block("value_set").await?
    );
    println!("Done!");
    Ok(())
}

/// Fetch `path`, parse it as JSON, print `GET <path>` + the pretty body, and
/// return the parsed value.
async fn get_json(addr: SocketAddr, path: &str) -> Result<Value> {
    let value = get_value(addr, path).await?;
    print_labeled(&format!("GET {path}"), &value);
    Ok(value)
}

/// Fetch `path` and parse the JSON body without printing it.
async fn get_value(addr: SocketAddr, path: &str) -> Result<Value> {
    let (status, body) = http_get(addr, path).await?;
    let value: Value = serde_json::from_str(&body)
        .with_context(|| format!("GET {path} returned a non-JSON body (HTTP {status}): {body}"))?;
    Ok(value)
}

/// Print a labeled, pretty-printed JSON block.
fn print_labeled(label: &str, value: &Value) {
    println!("{label}");
    println!("{}\n", serde_json::to_string_pretty(value).unwrap());
}

/// A bare-bones HTTP/1.1 `GET` over a raw TCP socket — enough to read the small
/// JSON responses the serving layer returns, with zero extra dependencies.
///
/// We send `Connection: close` so the server closes the socket after replying,
/// letting us read the whole response to EOF and split headers from body.
async fn http_get(addr: SocketAddr, path: &str) -> Result<(u16, String)> {
    let mut socket = TcpStream::connect(addr).await?;
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nAccept: application/json\r\n\r\n"
    );
    socket.write_all(request.as_bytes()).await?;

    let mut raw = Vec::new();
    socket.read_to_end(&mut raw).await?;
    let response = String::from_utf8_lossy(&raw);

    let (head, body) = response
        .split_once("\r\n\r\n")
        .context("malformed HTTP response (no header/body separator)")?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .context("could not parse HTTP status line")?;
    Ok((status, body.to_string()))
}
