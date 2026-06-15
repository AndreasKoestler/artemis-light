//! Integration tests for the opt-in serving layer.
//!
//! Compiled only under the `serving` feature so `cargo test` (default features)
//! is unaffected (serving-layer.OPTIN.2). Handlers are exercised in-process via
//! `Router::oneshot` against a temp-file SQLite database seeded through the real
//! `SqliteStore` writer.
#![cfg(feature = "serving")]

use std::net::SocketAddr;

use artemis_light::ServingLayer;
use artemis_light::persistence::{Row, SqlType, SqlValue, SqliteStore, Store, TableSchema};
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::Response;
use tower::ServiceExt; // for `oneshot`

/// Ephemeral bind address (unused by oneshot, required by the builder).
fn any_addr() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// Decode a JSON response body.
async fn body_json(resp: Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// Create a real (empty) SQLite database file at `path` via the writer store,
/// then close it so the serving layer can open it read-only.
async fn seed_empty_db(path: &str) {
    let store = SqliteStore::connect(&format!("sqlite:{path}"))
        .await
        .expect("connect writer store");
    drop(store);
}

/// Seed a temp DB through the real writer with two event tables:
/// `value_set` (block 100, value=7) and `transfer` (block 50, amount=3).
/// Also creates `_artemis_progress` (value_set→100, transfer→50).
async fn seed_two_tables(path: &str) {
    let store = SqliteStore::connect(&format!("sqlite:{path}"))
        .await
        .expect("connect writer store");
    let value_set = TableSchema::new("value_set")
        .col("value", SqlType::Integer)
        .col("_payload", SqlType::Text);
    store
        .write_block(
            &value_set,
            100,
            vec![Row(vec![
                SqlValue::Integer(7),
                SqlValue::Text("{\"value\":\"7\"}".to_string()),
            ])],
        )
        .await
        .unwrap();
    let transfer = TableSchema::new("transfer")
        .col("amount", SqlType::Integer)
        .col("_payload", SqlType::Text);
    store
        .write_block(
            &transfer,
            50,
            vec![Row(vec![
                SqlValue::Integer(3),
                SqlValue::Text("{\"amount\":\"3\"}".to_string()),
            ])],
        )
        .await
        .unwrap();
    drop(store);
}

/// Seed `value_set` with one row per `(block, value)` via the real writer.
async fn seed_value_set_blocks(path: &str, rows: &[(u64, i64)]) {
    let store = SqliteStore::connect(&format!("sqlite:{path}"))
        .await
        .expect("connect writer store");
    let schema = TableSchema::new("value_set")
        .col("value", SqlType::Integer)
        .col("_payload", SqlType::Text);
    for (block, value) in rows {
        store
            .write_block(
                &schema,
                *block,
                vec![Row(vec![
                    SqlValue::Integer(*value),
                    SqlValue::Text(format!("{{\"value\":\"{value}\"}}")),
                ])],
            )
            .await
            .unwrap();
    }
    drop(store);
}

/// Build a serving-layer router over the read-only DB at `path`.
async fn router_for(path: &str) -> axum::Router {
    ServingLayer::new(format!("sqlite:{path}"), any_addr())
        .into_router()
        .await
        .expect("router")
}

/// Issue a GET against a clone of `router` (oneshot consumes the service).
async fn get(router: &axum::Router, uri: &str) -> Response {
    router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn health_returns_ok_on_reachable_db() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.db");
    let url = path.to_str().unwrap().to_string();
    seed_empty_db(&url).await;

    let app = ServingLayer::new(format!("sqlite:{url}"), any_addr())
        .into_router()
        .await
        .expect("router");
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, serde_json::json!({"status": "ok"}));
}

#[tokio::test]
async fn serve_setup_errors_on_missing_db() {
    // create_if_missing(false): a missing file is a hard startup error, not a
    // silently-created empty DB. This is the user-visible "unavailable" surface.
    let res = ServingLayer::new("sqlite:/nonexistent-dir/none.db", any_addr())
        .into_router()
        .await;
    let err = res.expect_err("missing DB must fail");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("cannot start serving layer"),
        "error should carry the startup prefix, got: {msg}"
    );
}

#[tokio::test]
async fn tables_lists_seeded_event_tables_sorted() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await;

    let router = router_for(&url).await;
    let resp = get(&router, "/tables").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({ "tables": ["transfer", "value_set"] })
    );
}

#[tokio::test]
async fn tables_empty_on_fresh_db() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_empty_db(&url).await;

    let router = router_for(&url).await;
    let resp = get(&router, "/tables").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, serde_json::json!({ "tables": [] }));
}

#[tokio::test]
async fn schema_returns_columns_in_declared_order() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await;

    let router = router_for(&url).await;
    let resp = get(&router, "/tables/value_set/schema").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({
            "table": "value_set",
            "columns": [
                { "name": "block_number", "type": "INTEGER" },
                { "name": "value", "type": "INTEGER" },
                { "name": "_payload", "type": "TEXT" }
            ]
        })
    );
}

#[tokio::test]
async fn schema_unknown_table_is_404() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await;

    let router = router_for(&url).await;
    let resp = get(&router, "/tables/nope/schema").await;

    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({ "error": "unknown table: nope" })
    );
}

#[tokio::test]
async fn rows_returns_ascending_with_nested_payload() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_value_set_blocks(&url, &[(102, 9), (100, 7), (101, 8)]).await;

    let router = router_for(&url).await;
    let body = body_json(get(&router, "/tables/value_set/rows?limit=100").await).await;

    assert_eq!(body["limit"], serde_json::json!(100));
    assert_eq!(body["offset"], serde_json::json!(0));
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 3);
    // ascending block order regardless of insertion order
    assert_eq!(rows[0]["block_number"], serde_json::json!(100));
    assert_eq!(rows[1]["block_number"], serde_json::json!(101));
    assert_eq!(rows[2]["block_number"], serde_json::json!(102));
    assert_eq!(rows[0]["value"], serde_json::json!(7));
    // _payload surfaced as nested JSON, not a string
    assert_eq!(rows[0]["_payload"], serde_json::json!({ "value": "7" }));
}

#[tokio::test]
async fn rows_paging_and_block_range() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_value_set_blocks(&url, &[(100, 7), (101, 8), (102, 9)]).await;
    let router = router_for(&url).await;

    // limit + offset: second page of size 1 is block 101
    let body = body_json(get(&router, "/tables/value_set/rows?limit=1&offset=1").await).await;
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["block_number"], serde_json::json!(101));

    // inclusive block range
    let body = body_json(
        get(
            &router,
            "/tables/value_set/rows?from_block=101&to_block=101",
        )
        .await,
    )
    .await;
    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["block_number"], serde_json::json!(101));

    // from_block beyond data -> empty page, still 200
    let resp = get(&router, "/tables/value_set/rows?from_block=500").await;
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["rows"], serde_json::json!([]));
}

#[tokio::test]
async fn rows_invalid_limit_is_400_with_no_rows() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_value_set_blocks(&url, &[(100, 7)]).await;
    let router = router_for(&url).await;

    let resp = get(&router, "/tables/value_set/rows?limit=-1").await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_json(resp).await;
    assert_eq!(
        body,
        serde_json::json!({ "error": "invalid query parameter: limit" })
    );
    assert!(body.get("rows").is_none());
}

#[tokio::test]
async fn rows_unknown_table_is_404() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await;
    let router = router_for(&url).await;

    let resp = get(&router, "/tables/nope/rows").await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({ "error": "unknown table: nope" })
    );
}

#[tokio::test]
async fn status_returns_per_table_watermarks() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await; // value_set→100, transfer→50

    let router = router_for(&url).await;
    let resp = get(&router, "/status").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        body_json(resp).await,
        serde_json::json!({
            "tables": [
                { "table": "transfer", "last_block": 50 },
                { "table": "value_set", "last_block": 100 }
            ]
        })
    );
}

#[tokio::test]
async fn status_empty_before_any_write() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_empty_db(&url).await;

    let router = router_for(&url).await;
    let resp = get(&router, "/status").await;

    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await, serde_json::json!({ "tables": [] }));
}

// ---- Phase 6: lifecycle, concurrency, isolation, non-goals ----

#[tokio::test]
async fn serve_shuts_down_gracefully_and_releases_address() {
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio_util::sync::CancellationToken;

    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_empty_db(&url).await;

    // Reserve a free port, release it, then hand it to serve().
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let token = CancellationToken::new();
    let server = {
        let token = token.clone();
        let url = url.clone();
        tokio::spawn(async move {
            ServingLayer::new(format!("sqlite:{url}"), addr)
                .serve(token)
                .await
        })
    };

    // Connect once the listener is up, then GET /health over a raw socket.
    let mut stream = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr).await {
            stream = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut stream = stream.expect("serving layer should be listening");
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut resp = String::new();
    stream.read_to_string(&mut resp).await.unwrap();
    assert!(resp.contains("200 OK"), "expected 200, got: {resp}");
    assert!(
        resp.contains("\"status\":\"ok\""),
        "expected ok body, got: {resp}"
    );

    // Cancel → serve() returns Ok, address is released.
    token.cancel();
    let joined = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve should return promptly after cancel")
        .expect("serve task join");
    joined.expect("serve returns Ok on graceful shutdown");

    assert!(
        std::net::TcpListener::bind(addr).is_ok(),
        "address must be free after graceful shutdown"
    );
}

#[tokio::test]
async fn reads_stay_consistent_during_concurrent_writes() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_value_set_blocks(&url, &[(1, 1)]).await; // table exists with block 1

    let router = router_for(&url).await;

    // Writer commits blocks 2..=50 concurrently with reads.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        let store = SqliteStore::connect(&format!("sqlite:{writer_url}"))
            .await
            .unwrap();
        let schema = TableSchema::new("value_set")
            .col("value", SqlType::Integer)
            .col("_payload", SqlType::Text);
        for b in 2..=50u64 {
            store
                .write_block(
                    &schema,
                    b,
                    vec![Row(vec![
                        SqlValue::Integer(b as i64),
                        SqlValue::Text(format!("{{\"value\":\"{b}\"}}")),
                    ])],
                )
                .await
                .expect("writer commit must succeed despite concurrent reads");
        }
    });

    // Each read must return a contiguous committed prefix 1..=N — never a gap
    // or a partially-written block.
    for _ in 0..30 {
        let body = body_json(get(&router, "/tables/value_set/rows?limit=1000").await).await;
        let rows = body["rows"].as_array().unwrap();
        for (i, row) in rows.iter().enumerate() {
            assert_eq!(
                row["block_number"],
                serde_json::json!((i as i64) + 1),
                "rows must be a gap-free committed prefix"
            );
        }
    }

    writer.await.unwrap();

    // After the writer finishes, a read sees all 50 committed blocks.
    let body = body_json(get(&router, "/tables/value_set/rows?limit=1000").await).await;
    assert_eq!(body["rows"].as_array().unwrap().len(), 50);
}

#[tokio::test]
async fn reads_do_not_mutate_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_value_set_blocks(&url, &[(100, 7), (101, 8)]).await;

    {
        let router = router_for(&url).await;
        // Snapshot the served data, hammer every endpoint, then re-snapshot:
        // reads must be side-effect-free (no rows/tables/watermarks changed).
        let status_before = body_json(get(&router, "/status").await).await;
        let rows_before = body_json(get(&router, "/tables/value_set/rows?limit=100").await).await;

        for _ in 0..10 {
            let _ = get(&router, "/health").await;
            let _ = get(&router, "/tables").await;
            let _ = get(&router, "/tables/value_set/schema").await;
            let _ = get(&router, "/tables/value_set/rows?limit=100").await;
            let _ = get(&router, "/status").await;
        }

        let status_after = body_json(get(&router, "/status").await).await;
        let rows_after = body_json(get(&router, "/tables/value_set/rows?limit=100").await).await;
        assert_eq!(
            status_before, status_after,
            "watermarks changed under reads"
        );
        assert_eq!(rows_before, rows_after, "row data changed under reads");
    } // read pool dropped

    // Re-open with the writer: the watermark is exactly what was seeded — the
    // serving layer advanced nothing (serving-layer.READONLY.1).
    let store = SqliteStore::connect(&format!("sqlite:{url}"))
        .await
        .unwrap();
    assert_eq!(store.last_block("value_set").await.unwrap(), Some(101));
}

#[tokio::test]
async fn in_memory_database_is_rejected() {
    let res = ServingLayer::new("sqlite::memory:", any_addr())
        .into_router()
        .await;
    let err = res.expect_err("in-memory DB must be rejected");
    let msg = format!("{err:#}");
    assert!(
        msg.contains("cannot start serving layer") && msg.contains("in-memory"),
        "expected in-memory rejection, got: {msg}"
    );
}

#[tokio::test]
async fn mutating_methods_are_not_served() {
    // Non-goal confirmation: only GET routes exist; a write verb on a known path
    // is 405 Method Not Allowed (no mutating/raw-SQL surface).
    let dir = tempfile::tempdir().unwrap();
    let url = dir.path().join("events.db").to_str().unwrap().to_string();
    seed_two_tables(&url).await;
    let router = router_for(&url).await;

    let resp = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/tables")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}
