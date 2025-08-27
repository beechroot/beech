//! SQLite Virtual Table Interface for Beech Prolly Trees
//!
//! This module provides a read-only SQLite virtual table implementation that allows
//! querying beech prolly trees using standard SQL syntax.
//!
//! # Usage
//!
//! ```sql
//! CREATE VIRTUAL TABLE my_table USING beech(
//!     'path/to/data',  -- Path to directory containing .bch files
//!     'source_name',   -- Source identifier (currently unused)
//!     'table_name'     -- Name of the table within the prolly tree
//! );
//!
//! SELECT * FROM my_table WHERE id > 100;
//! ```

use apache_avro::{AvroSchema, Schema};
use beech_core::query::IndexUsage;
use beech_core::{BeechError, Column, Id, NodeSource, Table};
use log::debug;
use rusqlite::Result;
use rusqlite::ffi::ErrorCode;
use rusqlite::types::ValueRef;
use rusqlite::vtab::{
    Context, Filters, IndexConstraintOp, IndexInfo, VTab, VTabConnection, VTabCursor,
    read_only_module, CreateVTab, VTabKind,
};
use std::collections::HashMap;
use std::ffi::c_int;
use std::path::PathBuf;
use std::sync::Arc;

mod store;

struct BeechTable {
    remote_table_name: String,
    source: Arc<dyn NodeSource>,
    data_path: PathBuf,
}
fn avro_type_to_sqlite_type(typ: &Schema) -> &str {
    use apache_avro::Schema::*;
    match typ {
        Boolean => "boolean",
        Int => "integer",
        Long => "integer",
        Float => "real",
        Double => "real",
        String => "text",
        Bytes => "blob",
        _ => "text",
    }
}

fn parse_options(args: &[String]) -> Result<HashMap<String, String>> {
    args.iter()
        .map(|s| {
            let pieces: Vec<&str> = s.splitn(2, "=").collect();
            match &pieces[..] {
                [k, v] => Ok((k.to_string(), v.to_string())),
                [k] => Ok((k.to_string(), "".to_string())),
                _ => Err(rusqlite::Error::InvalidParameterName(format!(
                    "Invalid option: {s}"
                ))),
            }
        })
        .collect()
}

fn to_column_spec(c: &Column) -> Option<String> {
    if c.name == "rowid" {
        None
    } else {
        let col_type = avro_type_to_sqlite_type(&c.typ);
        Some(format!("{} {}", c.name, col_type))
    }
}

fn to_hex(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for &b in data {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn from_hex(hex: &str) -> beech_core::Result<Vec<u8>> {
    fn decode_nibble(c: u8) -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    }

    let bytes = hex.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(BeechError::Corrupt(
            "hex string must have even length".to_string(),
        ));
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    for i in (0..bytes.len()).step_by(2) {
        let high =
            decode_nibble(bytes[i]).ok_or(BeechError::Corrupt("invalid hex digit".to_string()))?;
        let low = decode_nibble(bytes[i + 1])
            .ok_or(BeechError::Corrupt("invalid hex digit".to_string()))?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

fn parse_arg(a: &[u8]) -> Result<String> {
    String::from_utf8(a.to_vec()).map_err(|_| {
        rusqlite::Error::InvalidParameterName(format!(
            "Invalid argument: {}",
            String::from_utf8_lossy(a)
        ))
    })
}

fn avro_value_from_sqlite(value: ValueRef<'_>) -> beech_core::Result<apache_avro::types::Value> {
    use apache_avro::types::Value;
    match value {
        ValueRef::Null => Ok(Value::Null),
        ValueRef::Integer(i) => Ok(Value::Long(i)),
        ValueRef::Real(f) => Ok(Value::Double(f)),
        ValueRef::Text(s) => Ok(Value::String(
            String::from_utf8(s.to_vec())
                .map_err(|_| BeechError::Corrupt("Invalid UTF-8".to_string()))?,
        )),
        ValueRef::Blob(b) => Ok(Value::Bytes(b.to_vec())),
    }
}

impl BeechTable {
    fn do_connect(
        _db: &mut VTabConnection,
        _module_name: &str,
        local_table_name: &str,
        data_path: &str,
        _source_name: &str,
        remote_table_name: &str,
        _options: &HashMap<String, String>,
    ) -> beech_core::Result<(String, Self)> {
        let data_path = PathBuf::from(data_path);

        // Create FileStore with proper .bch extension
        let store = store::file::FileStore::new(data_path.clone(), |key: &Id| {
            PathBuf::from(format!("{}.bch", key))
        });
        let source = beech_core::source::LocalFile::new(store);

        let column_spec = {
            // Read the root file to get current transaction ID
            let root_file_path = data_path.join("root");
            let transaction_id_str = std::fs::read_to_string(&root_file_path).map_err(|e| {
                BeechError::NotFound(format!(
                    "Failed to read root file at {}: {}",
                    root_file_path.display(),
                    e
                ))
            })?;

            let transaction_id = Id::from_hex(transaction_id_str.trim()).map_err(|e| {
                BeechError::Corrupt(format!("Invalid transaction ID in root file: {}", e))
            })?;

            let transaction = source.get_transaction(&transaction_id)?;
            let tab = source.get_table(&transaction, remote_table_name)?;

            let columns: Vec<String> = tab.columns().iter().filter_map(to_column_spec).collect();
            columns.join(", ")
        };
        let create_sql = format!("CREATE TABLE {local_table_name} ({column_spec});");
        Ok((
            create_sql,
            BeechTable {
                remote_table_name: remote_table_name.to_string(),
                source: Arc::new(source),
                data_path,
            },
        ))
    }
    fn do_best_index(&self, info: &mut IndexInfo) -> beech_core::Result<()> {
        // Read the root file to get current transaction ID
        let root_file_path = self.data_path.join("root");
        let transaction_id_str = std::fs::read_to_string(&root_file_path)
            .map_err(|e| BeechError::NotFound(format!("Failed to read root file: {}", e)))?;

        let transaction_id = Id::from_hex(transaction_id_str.trim())?;
        let transaction = self.source.get_transaction(&transaction_id)?;
        let table = self
            .source
            .get_table(&transaction, &self.remote_table_name)?;
        let mut argv_index = 1;
        let mut index_usage = beech_core::query::IndexUsage::new(table.id.clone());

        for (constraint, mut usage) in info.constraints_and_usages() {
            let should_use = index_usage.constraint(
                &table,
                constraint.column(),
                from_sqlite_op(constraint.operator()),
            );
            if should_use {
                usage.set_argv_index(argv_index);
                argv_index += 1;
            }
        }

        //convert 'constraints'to bytes using avro
        let schema = IndexUsage::get_schema();
        let mut writer = apache_avro::Writer::new(&schema, Vec::new());
        writer.append_ser(&index_usage)?;
        let bytes = writer.into_inner()?;
        let idx_str = to_hex(&bytes);

        info.set_idx_str(idx_str.as_str());
        Ok(())
    }
    fn do_open(&self) -> beech_core::Result<BeechCursor> {
        // Read the root file to get current transaction ID
        let root_file_path = self.data_path.join("root");
        let transaction_id_str = std::fs::read_to_string(&root_file_path)
            .map_err(|e| BeechError::NotFound(format!("Failed to read root file: {}", e)))?;

        let transaction_id = Id::from_hex(transaction_id_str.trim())?;
        let transaction = self.source.get_transaction(&transaction_id)?;
        let table = self
            .source
            .get_table(&transaction, &self.remote_table_name)?;
        let cursor = BeechCursor::new(&table, Arc::clone(&self.source));
        Ok(cursor)
    }
}

fn from_sqlite_op(op: IndexConstraintOp) -> beech_core::query::ConstraintOp {
    use IndexConstraintOp::*;
    match op {
        SQLITE_INDEX_CONSTRAINT_EQ => beech_core::query::ConstraintOp::Eq,
        SQLITE_INDEX_CONSTRAINT_GT => beech_core::query::ConstraintOp::Gt,
        SQLITE_INDEX_CONSTRAINT_LE => beech_core::query::ConstraintOp::Le,
        SQLITE_INDEX_CONSTRAINT_LT => beech_core::query::ConstraintOp::Lt,
        SQLITE_INDEX_CONSTRAINT_GE => beech_core::query::ConstraintOp::Ge,
        _ => beech_core::query::ConstraintOp::Unknown,
    }
}

unsafe impl<'vtab> VTab<'vtab> for BeechTable {
    type Aux = ();
    type Cursor = BeechCursor;

    fn connect(
        db: &mut VTabConnection,
        _aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        match &args
            .iter()
            .map(|a| parse_arg(a))
            .collect::<Result<Vec<String>>>()?[..]
        {
            [
                module_name_arg,        // args[0] = "beech"
                _database_name_arg,     // args[1] = "main" (ignore)
                local_table_name_arg,   // args[2] = "test_table"
                data_path_arg,          // args[3] = "/tmp/test_data"
                source_arg,             // args[4] = "test_source"
                remote_table_arg,       // args[5] = "table"
                option_args @ ..,
            ] => {
                let options = parse_options(option_args).map_err(|_| {
                    rusqlite::Error::InvalidParameterName(format!(
                        "Invalid options: {}",
                        option_args
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(" ")
                    ))
                })?;

                Self::do_connect(
                    db,
                    module_name_arg,
                    local_table_name_arg,
                    data_path_arg,      // Now correctly points to args[3]
                    source_arg,
                    remote_table_arg,
                    &options,
                )
                .map_err(into_rusqlite_error)
            }
            _ => Err(rusqlite::Error::InvalidParameterName(
                "Usage: CREATE VIRTUAL TABLE name USING beech(data_path, source_name, table_name [, options...])".to_string()
            )),
        }
    }

    fn best_index(&self, info: &mut IndexInfo) -> Result<()> {
        self.do_best_index(info).map_err(into_rusqlite_error)
    }
    fn open(&'vtab mut self) -> Result<Self::Cursor> {
        self.do_open().map_err(into_rusqlite_error)
    }
}

impl<'vtab> CreateVTab<'vtab> for BeechTable {
    const KIND: VTabKind = VTabKind::Default;
    
    fn create(
        db: &mut VTabConnection,
        aux: Option<&Self::Aux>,
        args: &[&[u8]],
    ) -> Result<(String, Self)> {
        // For our virtual table, create and connect are the same
        Self::connect(db, aux, args)
    }
}

fn into_rusqlite_error(be: BeechError) -> rusqlite::Error {
    match be {
        BeechError::NotFound(s) => rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::NotFound,
                extended_code: 0,
            },
            Some(s),
        ),
        _ => rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error {
                code: ErrorCode::DatabaseCorrupt,
                extended_code: 0,
            },
            Some(be.to_string()),
        ),
    }
}

struct BeechCursor {
    cursor: beech_core::query::Cursor,
    source: Arc<dyn NodeSource>,
}

impl BeechCursor {
    fn new(table: &Table, source: Arc<dyn NodeSource>) -> Self {
        Self {
            cursor: beech_core::query::Cursor::new(table),
            source,
        }
    }

    fn get_current_row(&self) -> beech_core::Result<Option<Vec<apache_avro::types::Value>>> {
        if let Some((page_id, row_idx)) = self.cursor.current() {
            let page = self.source.get_page(
                page_id,
                &self.cursor.table.key_scheme,
                &self.cursor.table.row_scheme,
            )?;

            match &*page {
                beech_core::Page::Leaf { rows, .. } => {
                    if let Some(row) = rows.get(*row_idx) {
                        // Row is (i64, Vec<Value>) - get the values
                        Ok(Some(row.1.clone()))
                    } else {
                        Ok(None)
                    }
                }
                beech_core::Page::Branch { .. } => Err(BeechError::Corrupt(
                    "Cursor pointing to branch node".to_string(),
                )),
            }
        } else {
            Ok(None)
        }
    }
    fn do_filter(
        &mut self,
        _idx_num: c_int,
        maybe_idx_str: Option<&str>,
        args: &Filters<'_>,
    ) -> beech_core::Result<()> {
        // Parse the index usage from the encoded string if provided by xBestIndex
        let index_usage = if let Some(idx_str) = maybe_idx_str {
            let bytes = from_hex(idx_str)?;
            let schema = IndexUsage::get_schema();
            let mut reader = apache_avro::Reader::with_schema(&schema, &bytes[..])
                .map_err(|e| BeechError::Corrupt(format!("Failed to create Avro reader: {e}")))?;

            if let Some(value_result) = reader.next() {
                let value = value_result.map_err(|e| {
                    BeechError::Corrupt(format!("Failed to decode IndexUsage: {e}"))
                })?;
                apache_avro::from_value::<IndexUsage>(&value).map_err(|e| {
                    BeechError::Corrupt(format!("Failed to deserialize IndexUsage: {e}"))
                })?
            } else {
                return Err(BeechError::Corrupt(
                    "No IndexUsage value in encoded data".to_string(),
                ));
            }
        } else {
            // Create a default IndexUsage if none provided
            IndexUsage::new(self.cursor.table.id.clone())
        };

        // Check schema compatibility
        if index_usage.table_id != self.cursor.table.id {
            return Err(BeechError::SchemaMismatch(
                "table schema mismatch".to_string(),
            ));
        }
        debug!("beech_filter(): schema matches");

        // Convert SQLite constraint values to Avro values
        // These values correspond to the constraints identified by xBestIndex
        let mut values = vec![];
        for (i, value_ref) in args.iter().enumerate() {
            debug!("Converting constraint value {i}: {value_ref:?}");
            let avro_value = avro_value_from_sqlite(value_ref)?;
            values.push(avro_value);
        }

        // Initialize the cursor with constraints and values
        self.cursor.init(index_usage.constraints, values);

        // Position the cursor at the first matching row
        self.cursor.advance_to_left(&*self.source)?;

        Ok(())
    }
}

unsafe impl VTabCursor for BeechCursor {
    // Required methods
    fn filter(&mut self, idx_num: c_int, idx_str: Option<&str>, args: &Filters<'_>) -> Result<()> {
        self.do_filter(idx_num, idx_str, args)
            .map_err(into_rusqlite_error)
    }
    fn next(&mut self) -> Result<()> {
        self.cursor.next(&*self.source).map_err(into_rusqlite_error)
    }
    fn eof(&self) -> bool {
        self.cursor.eof()
    }
    fn column(&self, ctx: &mut Context, i: c_int) -> Result<()> {
        match self.get_current_row() {
            Ok(Some(values)) => {
                if let Some(field_value) = values.get(i as usize) {
                    // Convert Avro value to SQLite value
                    match field_value {
                        apache_avro::types::Value::Null => {
                            ctx.set_result(&rusqlite::types::Null)?
                        }
                        apache_avro::types::Value::Boolean(b) => ctx.set_result(b)?,
                        apache_avro::types::Value::Int(n) => ctx.set_result(n)?,
                        apache_avro::types::Value::Long(n) => ctx.set_result(n)?,
                        apache_avro::types::Value::Float(f) => ctx.set_result(f)?,
                        apache_avro::types::Value::Double(f) => ctx.set_result(f)?,
                        apache_avro::types::Value::Bytes(b) => ctx.set_result(b)?,
                        apache_avro::types::Value::String(s) => ctx.set_result(s)?,
                        _ => {
                            // For complex types, convert to string representation
                            ctx.set_result(&format!("{field_value:?}"))?
                        }
                    }
                } else {
                    ctx.set_result(&rusqlite::types::Null)?;
                }
                Ok(())
            }
            Ok(None) => {
                ctx.set_result(&rusqlite::types::Null)?;
                Ok(())
            }
            Err(e) => Err(into_rusqlite_error(e)),
        }
    }
    fn rowid(&self) -> Result<i64> {
        // For now, use the position in the cursor stack as row id
        // This is a simple implementation that may need refinement
        Ok(self
            .cursor
            .current()
            .map(|(id, idx)| {
                // Use a combination of the page ID hash and index as rowid
                use std::hash::{Hash, Hasher};
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                Hash::hash(id, &mut hasher);
                let hash = hasher.finish();
                ((hash as i64) << 16) | (*idx as i64)
            })
            .unwrap_or(0))
    }
}

/// Create and register the beech virtual table module with SQLite
pub fn create_beech_module(conn: &rusqlite::Connection) -> rusqlite::Result<()> {
    let module = read_only_module::<BeechTable>();
    conn.create_module::<BeechTable, _>("beech", module, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_argument_parsing() {
        // Test parse_options function
        let args = vec!["key1=value1".to_string(), "key2=value2".to_string()];
        let options = parse_options(&args).unwrap();

        assert_eq!(options.get("key1"), Some(&"value1".to_string()));
        assert_eq!(options.get("key2"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_hex_functions() {
        let data = vec![0x00, 0x01, 0x02, 0x03, 0x0a, 0x0f, 0xff];
        let hex = to_hex(&data);
        assert_eq!(hex, "000102030a0fff");

        let decoded = from_hex(&hex).unwrap();
        assert_eq!(decoded, data);

        // Test invalid hex
        assert!(from_hex("invalid").is_err());
        assert!(from_hex("0g").is_err());
    }
}
