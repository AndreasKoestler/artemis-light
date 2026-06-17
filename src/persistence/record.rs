//! The [`Record`]: the mapping between one event type and its SQL rows.
//!
//! A `Record` owns the table name, the column schema — declared via an
//! override or frozen from the first encoded event — and both directions of
//! the mapping: encode an event to a [`Row`], decode a stored payload back to
//! the event. It also owns the reserved-name invariant; the
//! [`Store`](super::Store) sees only the schemas and rows a `Record` produces.
//!
//! Without a declared schema the mapping is a best guess: the table name comes
//! from the event's Solidity signature, column names from the event's `serde`
//! field names, and column types are inferred from the serialised JSON. This
//! requires events to implement [`serde::Serialize`].

use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::OnceLock;

use alloy::sol_types::SolEvent;
use anyhow::{Context, Result};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::schema::{
    BLOCK_NUMBER_COLUMN, Column, PAYLOAD_COLUMN, Row, SqlType, SqlValue, TableSchema,
};

/// The mapping between the event type `E` and its SQL rows. See the
/// [module docs](crate::persistence).
pub struct Record<E> {
    table: String,
    columns: ColumnsSource,
    _event: PhantomData<fn() -> E>,
}

/// Where a [`Record`]'s event-field columns come from.
enum ColumnsSource {
    /// Declared via a schema override, validated at construction.
    Declared(Vec<Column>),
    /// Inferred from the first successfully encoded event, then frozen for
    /// the lifetime of the `Record`. The store's `CREATE TABLE IF NOT EXISTS`
    /// freezes the table on the first write anyway, so later events that
    /// would infer differently (e.g. a `u64` crossing `i64::MAX`) only vary
    /// in value affinity, which SQLite absorbs.
    Inferred(OnceLock<Vec<Column>>),
}

impl<E: SolEvent> Record<E> {
    /// A `Record` for `E`, honouring an optional schema override.
    ///
    /// With an override, its table name and columns are used: each column's
    /// value is looked up by name from the event's fields when encoding (a
    /// missing field becomes `NULL`), so columns may be renamed-away,
    /// reordered, or retyped without disturbing value alignment. Errs when
    /// the override names a reserved identifier (the implicit columns or the
    /// store's progress table).
    ///
    /// Without an override, the table name is derived from `E`'s Solidity
    /// signature and the columns are frozen from the first encoded event.
    pub fn new(override_: Option<TableSchema>) -> Result<Self> {
        let (table, columns) = match override_ {
            Some(schema) => {
                // An override colliding with an implicit column would produce
                // a `CREATE TABLE` with duplicate columns; surfacing it here
                // makes the failure a clear construction error rather than an
                // opaque SQL one.
                if let Err(reason) = schema.ensure_no_reserved_names() {
                    anyhow::bail!("invalid schema override: {reason}");
                }
                (schema.table, ColumnsSource::Declared(schema.columns))
            }
            None => (table_name::<E>(), ColumnsSource::Inferred(OnceLock::new())),
        };
        Ok(Self {
            table,
            columns,
            _event: PhantomData,
        })
    }
}

impl<E> Record<E> {
    /// The table this event type's rows are written to and replayed from.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// The write schema: the event-field columns plus the implicit
    /// [`PAYLOAD_COLUMN`]. Available immediately for a declared schema;
    /// `None` for an inferred one until the first successful [`encode`]
    /// freezes it.
    ///
    /// [`encode`]: Record::encode
    pub fn schema(&self) -> Option<TableSchema> {
        let columns = match &self.columns {
            ColumnsSource::Declared(columns) => columns.as_slice(),
            ColumnsSource::Inferred(lock) => lock.get()?.as_slice(),
        };
        let mut columns = columns.to_vec();
        columns.push(Column::new(PAYLOAD_COLUMN, SqlType::Text));
        Some(TableSchema {
            table: self.table.clone(),
            columns,
        })
    }

    /// The schema used to read back stored events for replay: the table name
    /// plus the single [`PAYLOAD_COLUMN`]. Needs no encoded event, so replay
    /// can run before any live event is seen.
    pub fn payload_schema(&self) -> TableSchema {
        TableSchema::new(self.table.clone()).col(PAYLOAD_COLUMN, SqlType::Text)
    }

    /// Encode one event as a [`Row`] aligned to the frozen columns (a column
    /// with no matching field becomes `NULL`), with the event's full JSON
    /// appended as the [`PAYLOAD_COLUMN`] for lossless replay.
    pub fn encode(&self, event: &E) -> Result<Row>
    where
        E: Serialize,
    {
        // One serialisation pass feeds both the field map and the payload
        // column; this runs once per event on the persistence hot path.
        let json = event_json(event)?;
        let fields = field_map(&json)?;
        let columns = self.freeze_columns(&fields)?;
        let mut values: Vec<SqlValue> = columns
            .iter()
            .map(|col| {
                fields
                    .get(&col.name)
                    .map(|(_, value)| value.clone())
                    .unwrap_or(SqlValue::Null)
            })
            .collect();
        values.push(SqlValue::Text(json.to_string()));
        Ok(Row(values))
    }

    /// Reconstruct an event from a stored [`PAYLOAD_COLUMN`] value.
    pub fn decode(&self, payload: &str) -> Result<E>
    where
        E: DeserializeOwned,
    {
        serde_json::from_str(payload).context("stored payload is not valid for this event type")
    }

    /// The frozen event-field columns, inferring them from `fields` on the
    /// first call when no schema was declared.
    fn freeze_columns(&self, fields: &BTreeMap<String, (SqlType, SqlValue)>) -> Result<&[Column]> {
        match &self.columns {
            ColumnsSource::Declared(columns) => Ok(columns),
            ColumnsSource::Inferred(lock) => {
                if let Some(columns) = lock.get() {
                    return Ok(columns);
                }
                for name in fields.keys() {
                    // The store adds BLOCK_NUMBER_COLUMN and `encode` appends
                    // PAYLOAD_COLUMN; an event field by either name would
                    // shadow them in `CREATE TABLE` and break every write.
                    if name == BLOCK_NUMBER_COLUMN || name == PAYLOAD_COLUMN {
                        anyhow::bail!(
                            "event field {name:?} is reserved for an implicit column \
                             the persistence layer adds to every table; rename it \
                             with a schema override"
                        );
                    }
                }
                let columns = fields
                    .iter()
                    .map(|(name, (ty, _))| Column::new(name.clone(), *ty))
                    .collect();
                // A concurrent encode may have frozen first; either winner
                // derived from the same event type, so the loser's set is a
                // no-op. `get_or_init` returns whichever set won, so the
                // frozen columns are always available without unwrapping.
                Ok(lock.get_or_init(|| columns).as_slice())
            }
        }
    }
}

/// The best-guess table name for an event type, derived from its Solidity
/// signature: `ValueSet(uint256)` -> `value_set`.
fn table_name<E: SolEvent>() -> String {
    let signature = E::SIGNATURE;
    let name = signature.split('(').next().unwrap_or(signature);
    to_snake_case(name)
}

/// Serialise an event to its JSON [`Value`] — the single serialisation pass
/// both the field map and the payload column are derived from.
fn event_json<E: Serialize>(event: &E) -> Result<Value> {
    serde_json::to_value(event).context("event is not serialisable to JSON")
}

/// Map an event's top-level fields to `name -> (best-guess type, value)`,
/// sorted by field name (deterministic across runs).
fn field_map(value: &Value) -> Result<BTreeMap<String, (SqlType, SqlValue)>> {
    let object = match value {
        Value::Object(map) => map,
        other => anyhow::bail!("event must serialise to a JSON object, got {other}"),
    };
    Ok(object
        .iter()
        .map(|(key, field)| (key.clone(), (infer_type(field), json_to_sql(field))))
        .collect())
}

/// Best-guess SQL type for a serialised field value.
fn infer_type(value: &Value) -> SqlType {
    match value {
        Value::Bool(_) => SqlType::Integer,
        Value::Number(n) if n.is_i64() => SqlType::Integer,
        // A u64 beyond i64::MAX exceeds SQLite's signed 64-bit integers; it is
        // stored as decimal text (see `json_to_sql`), so type it as text.
        Value::Number(n) if n.is_u64() => match n.as_u64() {
            Some(u) if u > i64::MAX as u64 => SqlType::Text,
            _ => SqlType::Integer,
        },
        Value::Number(_) => SqlType::Real,
        Value::String(_) => SqlType::Text,
        // Arrays, objects and null are stored as JSON text.
        _ => SqlType::Text,
    }
}

/// Convert a serialised field value into a [`SqlValue`].
fn json_to_sql(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(b) => SqlValue::Integer(*b as i64),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SqlValue::Integer(i)
            } else if let Some(u) = n.as_u64() {
                // Beyond SQLite's signed 64-bit integers: reinterpreting via
                // `as i64` would store u64::MAX as -1, handing SQL queries
                // silently wrong numbers. Spill to decimal text instead; the
                // `_payload` column already preserves the exact value.
                SqlValue::Text(u.to_string())
            } else {
                SqlValue::Real(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::String(s) => SqlValue::Text(s.clone()),
        // Arrays / objects: keep the JSON text representation.
        other => SqlValue::Text(other.to_string()),
    }
}

/// Convert `PascalCase` / `camelCase` to `snake_case`.
fn to_snake_case(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 4);
    for (i, ch) in s.char_indices() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn snake_case_splits_on_each_uppercase() {
        assert_eq!(to_snake_case("ValueSet"), "value_set");
        assert_eq!(to_snake_case("Transfer"), "transfer");
        assert_eq!(to_snake_case("ERC20Transfer"), "e_r_c20_transfer");
        // camelCase: no leading underscore.
        assert_eq!(to_snake_case("valueSet"), "value_set");
        // Already snake / single word are unchanged.
        assert_eq!(to_snake_case("value"), "value");
        assert_eq!(to_snake_case("already_snake"), "already_snake");
        assert_eq!(to_snake_case(""), "");
    }

    #[test]
    fn infer_type_is_a_best_guess_from_json() {
        assert_eq!(infer_type(&json!(true)), SqlType::Integer);
        assert_eq!(infer_type(&json!(7)), SqlType::Integer);
        // Too large for SQLite's signed 64-bit integers: stored as text.
        assert_eq!(infer_type(&json!(u64::MAX)), SqlType::Text);
        assert_eq!(infer_type(&json!(1.5)), SqlType::Real);
        assert_eq!(infer_type(&json!("0x2a")), SqlType::Text);
        // Arrays, objects and null fall back to JSON text.
        assert_eq!(infer_type(&json!([1, 2])), SqlType::Text);
        assert_eq!(infer_type(&json!({"a": 1})), SqlType::Text);
        assert_eq!(infer_type(&json!(null)), SqlType::Text);
    }

    #[test]
    fn json_to_sql_converts_scalars_directly() {
        assert_eq!(json_to_sql(&json!(null)), SqlValue::Null);
        assert_eq!(json_to_sql(&json!(true)), SqlValue::Integer(1));
        assert_eq!(json_to_sql(&json!(false)), SqlValue::Integer(0));
        assert_eq!(json_to_sql(&json!(-7)), SqlValue::Integer(-7));
        assert_eq!(json_to_sql(&json!(1.5)), SqlValue::Real(1.5));
        assert_eq!(
            json_to_sql(&json!("0x2a")),
            SqlValue::Text("0x2a".to_string())
        );
    }

    #[test]
    fn json_to_sql_spills_a_u64_above_i64_max_to_decimal_text() {
        // SQLite integers are signed 64-bit; reinterpreting via `as i64` would
        // store u64::MAX as -1, handing SQL queries silently wrong numbers
        // (the `_payload` column preserves the true value, so replay was never
        // affected — but queryable columns must not lie). Spill to a decimal
        // string instead.
        let big = u64::MAX;
        assert_eq!(json_to_sql(&json!(big)), SqlValue::Text(big.to_string()));
        // The i64 boundary itself still fits and stays an integer.
        assert_eq!(
            json_to_sql(&json!(i64::MAX as u64)),
            SqlValue::Integer(i64::MAX)
        );
    }

    #[test]
    fn json_to_sql_stores_compound_values_as_json_text() {
        assert_eq!(
            json_to_sql(&json!([1, 2])),
            SqlValue::Text("[1,2]".to_string())
        );
        assert_eq!(
            json_to_sql(&json!({"a": 1})),
            SqlValue::Text("{\"a\":1}".to_string())
        );
    }
}
