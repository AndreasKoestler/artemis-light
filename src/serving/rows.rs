//! Paged, block-range row queries for `GET /tables/{table}/rows`.
//!
//! Performance note: the writer creates event tables without an index on
//! `block_number`, so the `WHERE block_number BETWEEN … ORDER BY block_number`
//! query is a full scan + sort per request, and large `offset` values degrade
//! linearly. This is acceptable for operator/dashboard traffic over modest
//! tables; for very large archives a future revision could add a `block_number`
//! index in the writer or switch to keyset pagination (last-seen `block_number`)
//! instead of `OFFSET`. The serving layer is read-only and must not modify the
//! schema, so it cannot add the index itself. `offset` is intentionally
//! unbounded (a pagination control, not a request-rate limit; rate limiting is
//! out of scope).

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::SqlitePool;

use super::catalog;
use super::error::ServingError;
use super::json;

/// Raw query parameters, parsed as strings so invalid values can be reported
/// precisely as [`ServingError::InvalidQuery`] rather than via axum's default
/// rejection.
#[derive(Deserialize)]
pub(crate) struct RowsQueryParams {
    from_block: Option<String>,
    to_block: Option<String>,
    limit: Option<String>,
    offset: Option<String>,
}

/// Resolved, validated query bounds.
pub(crate) struct Bounds {
    pub from_block: u64,
    pub to_block: u64,
    pub limit: u64,
    pub offset: u64,
}

/// Success body for `GET /tables/{table}/rows`.
#[derive(Serialize)]
pub(crate) struct RowsResponse {
    pub rows: Vec<Map<String, Value>>,
    pub limit: u64,
    pub offset: u64,
}

impl RowsQueryParams {
    /// Validate and resolve the parameters, applying defaults and clamping
    /// `limit` to `[1, max_limit]`. Any non-numeric / negative value yields
    /// `InvalidQuery(<name>)` (serving-layer.ERRORS.2).
    pub(crate) fn resolve(
        &self,
        default_limit: u64,
        max_limit: u64,
    ) -> Result<Bounds, ServingError> {
        let from_block = parse_u64("from_block", &self.from_block, 0)?;
        let to_block = parse_u64("to_block", &self.to_block, i64::MAX as u64)?;
        let limit = parse_u64("limit", &self.limit, default_limit)?.clamp(1, max_limit.max(1));
        let offset = parse_u64("offset", &self.offset, 0)?;
        Ok(Bounds {
            from_block,
            to_block,
            limit,
            offset,
        })
    }
}

fn parse_u64(name: &str, raw: &Option<String>, default: u64) -> Result<u64, ServingError> {
    match raw {
        None => Ok(default),
        Some(s) => s
            .trim()
            .parse::<u64>()
            .map_err(|_| ServingError::InvalidQuery(name.to_string())),
    }
}

/// Query a page of rows for a validated `table`, ordered by ascending block,
/// filtered to the inclusive `[from_block, to_block]` range (serving-layer.ROWS.1/.2/.4).
pub(crate) async fn query_rows(
    pool: &SqlitePool,
    table: &str,
    columns: &[(String, String)],
    bounds: &Bounds,
) -> anyhow::Result<Vec<Map<String, Value>>> {
    let sql = format!(
        "SELECT * FROM {} WHERE block_number BETWEEN ? AND ? \
         ORDER BY block_number ASC, rowid ASC LIMIT ? OFFSET ?",
        catalog::quote_ident(table)
    );
    let rows = sqlx::query(&sql)
        .bind(bounds.from_block as i64)
        .bind(bounds.to_block as i64)
        .bind(bounds.limit as i64)
        .bind(bounds.offset as i64)
        .fetch_all(pool)
        .await?;
    rows.iter().map(|r| json::row_to_json(r, columns)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(
        from: Option<&str>,
        to: Option<&str>,
        limit: Option<&str>,
        offset: Option<&str>,
    ) -> RowsQueryParams {
        RowsQueryParams {
            from_block: from.map(str::to_string),
            to_block: to.map(str::to_string),
            limit: limit.map(str::to_string),
            offset: offset.map(str::to_string),
        }
    }

    #[test]
    fn defaults_applied_when_absent() {
        let b = params(None, None, None, None).resolve(100, 1000).unwrap();
        assert_eq!(b.from_block, 0);
        assert_eq!(b.to_block, i64::MAX as u64);
        assert_eq!(b.limit, 100);
        assert_eq!(b.offset, 0);
    }

    #[test]
    fn limit_is_clamped_to_max() {
        let b = params(None, None, Some("5000"), None)
            .resolve(100, 1000)
            .unwrap();
        assert_eq!(b.limit, 1000);
        let b = params(None, None, Some("0"), None)
            .resolve(100, 1000)
            .unwrap();
        assert_eq!(b.limit, 1);
    }

    #[test]
    fn negative_or_non_numeric_is_invalid_query() {
        match params(None, None, Some("-1"), None).resolve(100, 1000) {
            Err(ServingError::InvalidQuery(name)) => assert_eq!(name, "limit"),
            _ => panic!("expected InvalidQuery(limit)"),
        }
        match params(Some("abc"), None, None, None).resolve(100, 1000) {
            Err(ServingError::InvalidQuery(name)) => assert_eq!(name, "from_block"),
            _ => panic!("expected InvalidQuery(from_block)"),
        }
    }
}
