//! Conversion of stored SQLite cells to JSON for row responses.
//!
//! `SqlValue` has no `Serialize` impl, so the serving layer maps each cell by
//! its declared column affinity. The `_payload` column (lossless event JSON) is
//! re-parsed into nested JSON, falling back to the raw string when unparsable
//! (maintainer-confirmed). `BLOB` cells are rendered as `0x`-prefixed lowercase
//! hex (Inferred: open question OQ-1 default).

use std::fmt::Write as _;

use serde_json::{Map, Number, Value};
use sqlx::Row;
use sqlx::sqlite::SqliteRow;

/// Name of the implicit lossless-event-JSON column added by the writer.
const PAYLOAD_COLUMN: &str = "_payload";

/// Convert one queried row to a JSON object keyed by column name, decoding each
/// cell by its declared `columns` affinity.
pub(crate) fn row_to_json(
    row: &SqliteRow,
    columns: &[(String, String)],
) -> anyhow::Result<Map<String, Value>> {
    let mut obj = Map::with_capacity(columns.len());
    for (name, ty) in columns {
        obj.insert(name.clone(), cell_to_json(row, name, ty)?);
    }
    Ok(obj)
}

fn cell_to_json(row: &SqliteRow, name: &str, ty: &str) -> anyhow::Result<Value> {
    let value = match ty.to_ascii_uppercase().as_str() {
        "INTEGER" => row
            .try_get::<Option<i64>, _>(name)?
            .map(Value::from)
            .unwrap_or(Value::Null),
        "REAL" => match row.try_get::<Option<f64>, _>(name)? {
            Some(f) => Number::from_f64(f)
                .map(Value::Number)
                .unwrap_or(Value::Null),
            None => Value::Null,
        },
        "BLOB" => row
            .try_get::<Option<Vec<u8>>, _>(name)?
            .map(|bytes| Value::String(to_hex(&bytes)))
            .unwrap_or(Value::Null),
        // TEXT, NUMERIC, and anything else decode as text.
        _ => match row.try_get::<Option<String>, _>(name)? {
            Some(s) if name == PAYLOAD_COLUMN => parse_payload(s),
            Some(s) => Value::String(s),
            None => Value::Null,
        },
    };
    Ok(value)
}

/// Surface `_payload` as nested JSON, or the raw string if it does not parse.
fn parse_payload(raw: String) -> Value {
    serde_json::from_str(&raw).unwrap_or(Value::String(raw))
}

/// Render bytes as a `0x`-prefixed lowercase hex string.
fn to_hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    #[tokio::test]
    async fn converts_cells_payload_and_blob() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE t (block_number INTEGER, value INTEGER, ratio REAL, \
             raw BLOB, missing INTEGER, _payload TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO t (block_number, value, ratio, raw, missing, _payload) \
             VALUES (100, 7, 1.5, x'00ff', NULL, '{\"value\":\"7\"}')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let columns = vec![
            ("block_number".to_string(), "INTEGER".to_string()),
            ("value".to_string(), "INTEGER".to_string()),
            ("ratio".to_string(), "REAL".to_string()),
            ("raw".to_string(), "BLOB".to_string()),
            ("missing".to_string(), "INTEGER".to_string()),
            ("_payload".to_string(), "TEXT".to_string()),
        ];
        let row = sqlx::query("SELECT * FROM t")
            .fetch_one(&pool)
            .await
            .unwrap();
        let obj = row_to_json(&row, &columns).unwrap();

        assert_eq!(obj["block_number"], serde_json::json!(100));
        assert_eq!(obj["value"], serde_json::json!(7));
        assert_eq!(obj["ratio"], serde_json::json!(1.5));
        assert_eq!(obj["raw"], serde_json::json!("0x00ff"));
        assert_eq!(obj["missing"], Value::Null);
        // _payload parsed into nested JSON, not a string.
        assert_eq!(obj["_payload"], serde_json::json!({ "value": "7" }));
    }

    #[test]
    fn payload_falls_back_to_raw_string_when_not_json() {
        assert_eq!(
            parse_payload("not json".to_string()),
            Value::String("not json".to_string())
        );
        assert_eq!(
            parse_payload("{\"a\":1}".to_string()),
            serde_json::json!({ "a": 1 })
        );
    }
}
