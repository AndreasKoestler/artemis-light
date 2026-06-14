//! Error taxonomy for the serving layer and its HTTP rendering.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

/// Uniform JSON error body returned for every error response.
#[derive(Serialize)]
pub struct ErrorBody {
    pub error: String,
}

/// Errors the serving layer can surface, each mapped to an HTTP status.
#[derive(Debug)]
pub enum ServingError {
    /// Requested table is not present in the catalog → 404 (serving-layer.ERRORS.1).
    UnknownTable(String),
    /// A query parameter failed validation → 400 (serving-layer.ERRORS.2).
    InvalidQuery(String),
    /// A read against the database failed → 500.
    Database(anyhow::Error),
    /// The database could not be reached on a liveness probe → 503.
    Unavailable,
}

impl IntoResponse for ServingError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            ServingError::UnknownTable(table) => {
                (StatusCode::NOT_FOUND, format!("unknown table: {table}"))
            }
            ServingError::InvalidQuery(param) => (
                StatusCode::BAD_REQUEST,
                format!("invalid query parameter: {param}"),
            ),
            ServingError::Database(_) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "database error".to_string(),
            ),
            ServingError::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "database unavailable".to_string(),
            ),
        };
        (status, Json(ErrorBody { error: message })).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn variant_status_mapping() {
        assert_eq!(
            ServingError::UnknownTable("nope".into())
                .into_response()
                .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ServingError::InvalidQuery("limit".into())
                .into_response()
                .status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            ServingError::Database(anyhow::anyhow!("boom"))
                .into_response()
                .status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            ServingError::Unavailable.into_response().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
    }
}
