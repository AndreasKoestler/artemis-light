//! Table schema and row value types shared by every [`Store`](super::Store).

/// Name of the implicit per-row block column every [`Store`](super::Store)
/// adds when writing.
pub(crate) const BLOCK_NUMBER_COLUMN: &str = "block_number";

/// Name of the implicit column holding each event's full JSON, used to
/// losslessly reconstruct the event when replaying from the database.
pub const PAYLOAD_COLUMN: &str = "_payload";

/// Name of the bookkeeping table [`SqliteStore`](super::SqliteStore) records
/// per-table progress in.
pub(crate) const PROGRESS_TABLE: &str = "_artemis_progress";

/// Quote a SQL identifier (table/column name) for inclusion in a statement by
/// wrapping it in double quotes and doubling any interior double quotes. Shared
/// by every [`Store`](super::Store) and the serving layer so identifier quoting
/// can never diverge between writer and reader.
pub(crate) fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// A SQL column type. SQLite is dynamically typed, so these act as type
/// affinities / a best-guess mapping from event field types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlType {
    Integer,
    Real,
    Text,
    Blob,
    Numeric,
}

impl SqlType {
    /// The SQL keyword used in a `CREATE TABLE` column definition.
    pub fn sql(&self) -> &'static str {
        match self {
            SqlType::Integer => "INTEGER",
            SqlType::Real => "REAL",
            SqlType::Text => "TEXT",
            SqlType::Blob => "BLOB",
            SqlType::Numeric => "NUMERIC",
        }
    }
}

/// A single column in a [`TableSchema`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub ty: SqlType,
}

impl Column {
    pub fn new(name: impl Into<String>, ty: SqlType) -> Self {
        Self {
            name: name.into(),
            ty,
        }
    }
}

/// The schema for one event's table: a table name and its event-field columns.
///
/// The `block_number` column is implicit — every [`Store`](super::Store) adds
/// it — so a schema describes only the event's own fields.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    pub table: String,
    pub columns: Vec<Column>,
}

impl TableSchema {
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            columns: Vec::new(),
        }
    }

    /// Append a column (builder style).
    pub fn col(mut self, name: impl Into<String>, ty: SqlType) -> Self {
        self.columns.push(Column::new(name, ty));
        self
    }

    /// Err when the schema collides with identifiers the persistence layer
    /// itself uses: the implicit [`BLOCK_NUMBER_COLUMN`] and [`PAYLOAD_COLUMN`]
    /// columns (a collision produces a `CREATE TABLE` with duplicate columns),
    /// or the internal [`PROGRESS_TABLE`] (a collision corrupts every table's
    /// progress watermark).
    pub(crate) fn ensure_no_reserved_names(&self) -> Result<(), String> {
        if self.table == PROGRESS_TABLE {
            return Err(format!(
                "table name {PROGRESS_TABLE:?} is reserved for the store's internal bookkeeping"
            ));
        }
        for column in &self.columns {
            if column.name == BLOCK_NUMBER_COLUMN || column.name == PAYLOAD_COLUMN {
                return Err(format!(
                    "column name {:?} is reserved for an implicit column the \
                     persistence layer adds to every table",
                    column.name
                ));
            }
        }
        Ok(())
    }
}

/// A scalar value bound into / read out of a SQL cell.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

/// One row's worth of event-field values, ordered to match
/// [`TableSchema::columns`].
#[derive(Debug, Clone, PartialEq)]
pub struct Row(pub Vec<SqlValue>);

#[cfg(test)]
mod tests {
    use super::*;

    // Construction helpers (`SqlType::sql`, `Column::new`, `TableSchema::new`)
    // are restatements of their own definitions; testing them asserts nothing
    // a regression could violate. Only the builder's ordering contract — which
    // value alignment in `derive_record_with` depends on — is worth pinning.

    #[test]
    fn table_schema_builder_appends_columns_in_order() {
        let schema = TableSchema::new("transfer")
            .col("from", SqlType::Text)
            .col("amount", SqlType::Numeric);

        assert_eq!(schema.table, "transfer");
        assert_eq!(
            schema.columns,
            vec![
                Column::new("from", SqlType::Text),
                Column::new("amount", SqlType::Numeric),
            ]
        );
    }
}
