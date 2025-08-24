use crate::{BeechError, Id, Page, Result, Root, Table, Transaction};
use apache_avro::schema::{ArraySchema, RecordSchema};
use apache_avro::types::Value;
use apache_avro::{AvroSchema, Schema, from_avro_datum, from_value, to_avro_datum, to_value};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Cursor, Read, Write};
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq, AvroSchema)]
pub struct WireRoot {
    pub id: Id,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, AvroSchema)]
pub struct WireTransaction {
    pub id: Id,
    pub prev_id: Id,
    pub transaction_time: i64, // microseconds since epoch
    pub tables: HashMap<String, Id>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, AvroSchema)]
pub struct WireTable {
    pub id: Id,
    pub name: String,
    pub timestamp: i64,
    pub key_scheme: String,
    pub row_scheme: String,
    pub root: Option<Id>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default, AvroSchema)]
struct WirePage {
    pub id: Id,
    #[serde(with = "serde_bytes")]
    pub keys: Vec<u8>,
    pub rowids: Vec<i64>,
    #[serde(with = "serde_bytes")]
    pub rows: Vec<u8>,
    pub children: Vec<Id>,
    pub depth: i32,
    pub row_count: i64,
}

fn is_record_scheme(schema: &Schema) -> bool {
    matches!(schema, Schema::Record(_))
}

fn wire_page_schema() -> Schema {
    // TODO: at some point, let's vendor int the Avro stuff,
    // so that we can control the output of AvroSchema.
    // otherwise mayebe we can convice the upstream maintainers
    // to add a flag to control type better.

    let original = WirePage::get_schema();
    if let Schema::Record(mut record) = original {
        // the default schema for the fields "keys" and "rows" is not "bytes",
        // but rather an array of integers.  Find those fields and replace them
        let fields = record.fields.iter().map(|field| {
            if &field.name == "keys" || &field.name == "rows" {
                let mut new_field = field.clone();
                new_field.schema = Schema::Bytes;
                new_field
            } else {
                field.clone()
            }
        });
        record.fields = fields.collect();
        Schema::Record(record)
    } else {
        panic!("expected record schema");
    }
}
fn array_scheme_from_scheme(schema: &Schema) -> Schema {
    Schema::Array(ArraySchema {
        items: Box::new(schema.clone()),
        attributes: Default::default(),
    })
}

pub fn encode_transaction(transaction: &Transaction) -> Result<Vec<u8>> {
    let wire_transaction = WireTransaction {
        id: transaction.id.clone(),
        prev_id: transaction.prev_id.clone(),
        transaction_time: transaction
            .transaction_time
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        tables: transaction.tables.clone(),
    };
    let bytes = to_avro_datum(&WireTransaction::get_schema(), to_value(wire_transaction)?)?;
    Ok(bytes)
}

pub fn decode_transaction<R: Read>(reader: &mut R) -> Result<Transaction> {
    let wire_transaction = from_avro_datum(&WireTransaction::get_schema(), reader, None)?;
    let transaction: WireTransaction = from_value(&wire_transaction)?;
    Ok(Transaction {
        id: transaction.id,
        prev_id: transaction.prev_id,
        transaction_time: SystemTime::UNIX_EPOCH
            + Duration::from_millis(transaction.transaction_time as u64),
        tables: transaction.tables,
    })
}
fn encode_array(scheme: &Schema, value: Vec<Value>) -> Result<Vec<u8>> {
    assert!(is_record_scheme(scheme));
    let array_scheme = array_scheme_from_scheme(scheme);
    let array = Value::Array(value);
    Ok(to_avro_datum(&array_scheme, array)?)
}
fn encode_record_array<W: Write>(
    writer: &mut W,
    scheme: &Schema,
    values: &[Vec<Value>],
) -> Result<()> {
    let Schema::Record(RecordSchema { fields, .. }) = scheme else {
        return Err(BeechError::Corrupt("expected record schema while encoding record array".to_string()));
    };
    let records: Result<Vec<_>> = values
        .iter()
        .map(|row| {
            if row.len() != fields.len() {
                return Err(BeechError::Corrupt("expected record with matching number of fields".to_string()));
            }
            Ok(Value::Record(
                fields
                    .iter()
                    .zip(row.iter())
                    .map(|(field, value)| (field.name.clone(), value.clone()))
                    .collect(),
            ))
        })
        .collect();
    let bytes = encode_array(scheme, records?)?;
    writer.write_all(&bytes)?;
    Ok(())
}
pub fn encode_page(
    page: &Page,
    id: Id,
    key_scheme: &Schema,
    row_scheme: &Schema,
) -> Result<Vec<u8>> {
    assert!(is_record_scheme(key_scheme));
    assert!(is_record_scheme(row_scheme));
    let mut wp = WirePage::default();
    wp.id = id;
    match page {
        Page::Branch { keys, children, depth, row_count } => {
            wp.children = children.to_vec();
            wp.depth = *depth;
            wp.row_count = *row_count;
            let mut writer = Vec::new();
            encode_record_array(&mut writer, key_scheme, keys)?;
            wp.keys = writer;
        }
        Page::Leaf { rows, .. } => {
            wp.depth = 0;  // Leaf pages always have depth 0
            wp.row_count = rows.len() as i64;  // Computed from rows
            let (rowids, rows): (Vec<_>, Vec<_>) = rows.clone().into_iter().unzip();
            wp.rowids = rowids;
            let mut writer = Vec::new();
            encode_record_array(&mut writer, row_scheme, &rows)?;
            wp.rows = writer;
        }
    }

    let bytes = to_avro_datum(&wire_page_schema(), to_value(wp)?)?;
    Ok(bytes)
}

fn try_from_array(value: Value) -> Result<Vec<Value>> {
    let Value::Array(values) = value else {
        return Err(BeechError::Corrupt("expected array".to_string()));
    };
    Ok(values)
}

fn decode_array(scheme: &Schema, data: &[u8]) -> Result<Vec<Value>> {
    let row_array_scheme = array_scheme_from_scheme(scheme);
    let mut reader = Cursor::new(data);
    let row_values = from_avro_datum(&row_array_scheme, &mut reader, None)?;
    try_from_array(row_values)
}

fn decode_record_array(scheme: &Schema, data: &[u8]) -> Result<Vec<Vec<Value>>> {
    let array = decode_array(scheme, data)?;
    array
        .into_iter()
        .map(|value| {
            let Value::Record(record) = value else {
                return Err(BeechError::Corrupt("expected record while decoding record array".to_string()));
            };
            Ok(record.into_iter().map(|(_name, value)| value).collect())
        })
        .collect()
}

pub fn decode_page<R: Read>(
    reader: &mut R,
    key_scheme: &Schema,
    row_scheme: &Schema,
) -> Result<Page> {
    let wp_value = from_avro_datum(&wire_page_schema(), reader, None)?;
    let wp: WirePage = from_value(&wp_value)?;
    let is_leaf = wp.children.is_empty();
    if is_leaf {
        let rows = decode_record_array(row_scheme, &wp.rows[..])?;
        if rows.len() != wp.rowids.len() {
            return Err(BeechError::Corrupt("expected matching number of rows and rowids".to_string()));
        }
        let key_columns: Vec<usize> = find_key_columns(key_scheme, row_scheme)?;
        let keys: Vec<Vec<_>> = rows
            .iter()
            .map(|row| key_from_row(&key_columns, row))
            .collect();
        let ids_and_rows: Vec<_> = wp.rowids.into_iter().zip(rows).collect();
        Ok(Page::Leaf {
            keys,
            rows: ids_and_rows,
        })
    } else {
        let keys: Vec<Vec<_>> = decode_record_array(key_scheme, &wp.keys[..])?;
        if keys.len() + 1 != wp.children.len() {
            return Err(BeechError::Corrupt("expected congruent number of keys and children".to_string()));
        }
        Ok(Page::Branch {
            keys,
            children: wp.children,
            depth: wp.depth,
            row_count: wp.row_count,
        })
    }
}

fn key_from_row(key_columns: &Vec<usize>, row: &Vec<Value>) -> Vec<Value> {
    key_columns.iter().map(|i| row[*i].clone()).collect()
}

pub fn find_key_columns(key_scheme: &Schema, row_scheme: &Schema) -> Result<Vec<usize>> {
    let key_columns = match (key_scheme, row_scheme) {
        (Schema::Record(key_record), Schema::Record(row_record)) => key_record
            .fields
            .iter()
            .filter_map(|field| {
                // Find the row column index for this key field
                row_record
                    .fields
                    .iter()
                    .position(|f| f.name == field.name)
            })
            .collect(),
        _ => return Err(BeechError::SchemaMismatch("expected record schema while finding key columns".to_string())),
    };
    Ok(key_columns)
}

// fn schema_to_string(schema: &Schema) -> Result<String> {
//     // use serde to serialize the schema
//     serde_json::to_string(schema).map_err(BeechError::Serialization)
// }

pub fn encode_table(table: &Table, timestamp: i64) -> Result<Vec<u8>> {
    let wire_table = WireTable {
        id: table.id.clone(),
        name: table.name.clone(),
        timestamp,
        key_scheme: table.key_scheme.canonical_form().to_string(),
        row_scheme: table.row_scheme.canonical_form().to_string(),
        root: table.root.clone(),
    };
    let bytes = to_avro_datum(&WireTable::get_schema(), to_value(wire_table)?)?;
    Ok(bytes)
}

pub fn decode_table<R: Read>(reader: &mut R) -> Result<Table> {
    let wire_table = from_avro_datum(&WireTable::get_schema(), reader, None)?;
    let wire_table: WireTable = from_value(&wire_table)?;

    let key_scheme = Schema::parse_str(&wire_table.key_scheme)?;
    let row_scheme = Schema::parse_str(&wire_table.row_scheme)?;
    Table::new(
        wire_table.id,
        wire_table.name,
        wire_table.root,
        key_scheme,
        row_scheme,
    )
}

pub fn encode_root(root: &Root) -> Result<Vec<u8>> {
    let wire_root = WireRoot {
        id: root.id.clone(),
    };
    let bytes = to_avro_datum(&WireRoot::get_schema(), to_value(wire_root)?)?;
    Ok(bytes)
}

pub fn decode_root<R: Read>(reader: &mut R) -> Result<Root> {
    let wire_root = from_avro_datum(&WireRoot::get_schema(), reader, None)?;
    let wire_root: WireRoot = from_value(&wire_root)?;
    Ok(Root { id: wire_root.id })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logger() {
        // do only once
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            simple_logger::SimpleLogger::new().env().init().unwrap();
        });
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, AvroSchema)]
    struct MyRecord {
        x: i32,
        y: i32,
    }
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, AvroSchema)]
    struct MyKey {
        x: i32,
    }

    #[test]
    fn test_page_roundtrip() {
        init_logger();
        let keys = vec![
            vec![Value::Int(1)],
            vec![Value::Int(2)],
            vec![Value::Int(3)],
        ];
        let rows = vec![
            (1000, vec![Value::Int(1), Value::Int(100)]),
            (1001, vec![Value::Int(2), Value::Int(200)]),
            (1002, vec![Value::Int(3), Value::Int(300)]),
        ];
        let leaf = Page::Leaf {
            keys: keys.clone(),
            rows,
        };
        let branch = Page::Branch {
            keys: keys.clone(),
            children: vec![10001.into(), 10002.into(), 10003.into(), 10004.into()],
            depth: 1,
            row_count: 3,
        };

        for page in [leaf, branch] {
            let key_scheme = MyKey::get_schema();
            let row_scheme = MyRecord::get_schema();
            let encoded = encode_page(&page, 9999.into(), &key_scheme, &row_scheme).unwrap();
            let decoded = decode_page(&mut Cursor::new(encoded), &key_scheme, &row_scheme).unwrap();
            assert_eq!(page, decoded);
        }
    }

    #[test]
    fn test_table_roundtrip() {
        init_logger();
        let table = Table::new(
            999.into(),
            "test".to_string(),
            None,
            Schema::parse_str(
                r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": "int"}] }"#,
            )
            .unwrap(),
            Schema::parse_str(
                r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": "int"}] }"#,
            )
            .unwrap(),
        )
        .unwrap();
        let encoded = encode_table(&table, 1234567890).unwrap();
        let decoded = decode_table(&mut Cursor::new(encoded)).unwrap();
        assert_eq!(table, decoded);
    }

    #[test]
    fn test_simple_nullable_field() {
        // ATTENTION: ["null", "int"] order matters!
        let schema = Schema::parse_str(r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": ["null", "int"]}] }"#).unwrap();
        #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
        struct Foo {
            x: Option<i32>,
        }
        let foo = Foo { x: Some(1) };
        let encoded = to_avro_datum(&schema, to_value(&foo).unwrap()).unwrap();
        let decoded = from_avro_datum(&schema, &mut Cursor::new(encoded), None).unwrap();
        assert_eq!(&from_value::<Foo>(&decoded).unwrap(), &foo);
    }
}
