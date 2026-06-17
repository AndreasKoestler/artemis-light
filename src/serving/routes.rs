//! axum router, shared state, and request handlers for the serving layer.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::response::IntoResponse;
use axum::routing::get;
use serde::Serialize;

use super::backend::ServingBackend;
use super::error::ServingError;
use super::rows::{RowsQueryParams, RowsResponse};

/// Shared handler state: the read-only storage backend plus the configured
/// paging limits. The backend is chosen by URL scheme in
/// [`ServingLayer::into_router`](super::ServingLayer::into_router).
#[derive(Clone)]
pub struct AppState {
    pub(crate) backend: Arc<dyn ServingBackend>,
    pub(crate) default_limit: u64,
    pub(crate) max_limit: u64,
}

impl AppState {
    pub(crate) fn new(
        backend: Arc<dyn ServingBackend>,
        default_limit: u64,
        max_limit: u64,
    ) -> Self {
        Self {
            backend,
            default_limit,
            max_limit,
        }
    }
}

/// Success body for `GET /health`.
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

/// Success body for `GET /tables`.
#[derive(Serialize)]
struct TablesResponse {
    tables: Vec<String>,
}

/// One column in a `GET /tables/{table}/schema` response.
#[derive(Serialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    col_type: String,
}

/// Success body for `GET /tables/{table}/schema`.
#[derive(Serialize)]
struct SchemaResponse {
    table: String,
    columns: Vec<ColumnInfo>,
}

/// One table's indexing watermark in a `GET /status` response.
#[derive(Serialize)]
struct TableStatus {
    table: String,
    last_block: u64,
}

/// Success body for `GET /status`.
#[derive(Serialize)]
struct StatusResponse {
    tables: Vec<TableStatus>,
}

/// Build the serving-layer router backed by `state`.
pub(crate) fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(get_health_handler))
        .route("/tables", get(list_tables_handler))
        .route("/tables/:table/schema", get(get_schema_handler))
        .route("/tables/:table/rows", get(query_rows_handler))
        .route("/status", get(get_status_handler))
        .with_state(state)
}

/// `GET /health` — liveness probe; 200 `{"status":"ok"}` when the database is
/// reachable, 503 otherwise (serving-layer.STATUS.2).
pub async fn get_health_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.backend.health().await {
        Ok(()) => Json(HealthResponse { status: "ok" }).into_response(),
        Err(_) => ServingError::Unavailable.into_response(),
    }
}

/// `GET /tables` — list the persisted event tables (serving-layer.TABLES.1/.3).
pub async fn list_tables_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.backend.list_tables().await {
        Ok(tables) => Json(TablesResponse { tables }).into_response(),
        Err(e) => ServingError::Database(e).into_response(),
    }
}

/// `GET /tables/{table}/schema` — column names and types for a known table
/// (serving-layer.TABLES.2/.3); 404 `UnknownTable` for an absent table.
pub async fn get_schema_handler(
    State(state): State<AppState>,
    Path(table): Path<String>,
) -> impl IntoResponse {
    match state.backend.table_exists(&table).await {
        Ok(true) => {}
        Ok(false) => return ServingError::UnknownTable(table).into_response(),
        Err(e) => return ServingError::Database(e).into_response(),
    }
    match state.backend.table_columns(&table).await {
        Ok(cols) => Json(SchemaResponse {
            table,
            columns: cols
                .into_iter()
                .map(|(name, col_type)| ColumnInfo { name, col_type })
                .collect(),
        })
        .into_response(),
        Err(e) => ServingError::Database(e).into_response(),
    }
}

/// `GET /tables/{table}/rows` — paged, block-range-filtered rows as JSON in
/// ascending block order (serving-layer.ROWS.1/.2/.3/.4); 404 for unknown table,
/// 400 for invalid query parameters.
pub async fn query_rows_handler(
    State(state): State<AppState>,
    Path(table): Path<String>,
    Query(params): Query<RowsQueryParams>,
) -> impl IntoResponse {
    match state.backend.table_exists(&table).await {
        Ok(true) => {}
        Ok(false) => return ServingError::UnknownTable(table).into_response(),
        Err(e) => return ServingError::Database(e).into_response(),
    }
    let bounds = match params.resolve(state.default_limit, state.max_limit) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    match state.backend.query_rows(&table, &bounds).await {
        Ok(rows) => Json(RowsResponse {
            rows,
            limit: bounds.limit,
            offset: bounds.offset,
        })
        .into_response(),
        Err(e) => ServingError::Database(e).into_response(),
    }
}

/// `GET /status` — per-table last-processed block from `_artemis_progress`
/// (serving-layer.STATUS.1); empty list before anything is written.
pub async fn get_status_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state.backend.watermarks().await {
        Ok(watermarks) => Json(StatusResponse {
            tables: watermarks
                .into_iter()
                .map(|(table, last_block)| TableStatus {
                    table,
                    last_block: last_block as u64,
                })
                .collect(),
        })
        .into_response(),
        Err(e) => ServingError::Database(e).into_response(),
    }
}
