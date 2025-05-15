use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use apache_avro::schema::derive::AvroSchemaComponent;
use apache_avro::schema::{FixedSchema, Name, Namespace, RecordField, RecordSchema, Schema};
use apache_avro::types::Value;
use thiserror::Error;
use beech_mem::mmap::MappedBuffer;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

#[cfg(feature = "serde")]
pub mod wire;
pub mod source;
pub mod query;

pub type Row = (i64, Vec<Value>); // TODO: Avro doesn't support unsigned 64-bit integers
pub type Key = Vec<Value>;

// Helper trait to add ordering to Key
pub trait KeyOrdering {
    fn compare_key(&self, other: &Self) -> std::cmp::Ordering;
    fn max_key(self, other: Self) -> Self where Self: Sized;
}

impl KeyOrdering for Key {
    fn compare_key(&self, other: &Self) -> std::cmp::Ordering {
        use crate::query::OrdValue;
        for (a, b) in self.iter().zip(other.iter()) {
            match OrdValue(a).partial_cmp(&OrdValue(b)) {
                Some(std::cmp::Ordering::Equal) => continue,
                Some(ord) => return ord,
                None => return std::cmp::Ordering::Equal, // fallback for incomparable values
            }
        }
        self.len().cmp(&other.len())
    }
    
    fn max_key(self, other: Self) -> Self {
        match self.compare_key(&other) {
            std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => self,
            std::cmp::Ordering::Less => other,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub key_index: Option<usize>,
    pub name: String,
    pub typ: Schema,
}

#[derive(std::hash::Hash, Default, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Id(#[serde(with = "serde_bytes")] [u8; 32]);

impl Id {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
    
    fn hex_string(&self) -> String {
        self.0
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<String>>()
            .join("")
    }

    pub fn from_hex(hex_str: &str) -> Result<Self> {
        if hex_str.len() != 64 {
            return Err(BeechError::Corrupt(format!("Invalid hex string length: expected 64, got {}", hex_str.len())));
        }
        
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex_str.as_bytes().chunks(2).enumerate() {
            if i >= 32 {
                return Err(BeechError::Corrupt("Hex string too long".to_string()));
            }
            let hex_pair = std::str::from_utf8(chunk)
                .map_err(|_| BeechError::Corrupt("Invalid UTF-8 in hex string".to_string()))?;
            bytes[i] = u8::from_str_radix(hex_pair, 16)
                .map_err(|_| BeechError::Corrupt(format!("Invalid hex digit: {hex_pair}")))?;
        }
        Ok(Id(bytes))
    }
}

impl AvroSchemaComponent for Id {
    fn get_schema_in_ctxt(
        named_schemas: &mut HashMap<Name, Schema>,
        enclosing_namespace: &Namespace,
    ) -> Schema {
        let name = apache_avro::schema::Name::new("Id")
            .expect("Unable to parse schema name")
            .fully_qualified_name(enclosing_namespace);
        // Check, if your name is already defined, and if so, return a ref to that name
        if named_schemas.contains_key(&name) {
            Schema::Ref { name: name.clone() }
        } else {
            named_schemas.insert(
                name.clone(),
                apache_avro::schema::Schema::Ref { name: name.clone() },
            );
            Schema::Fixed(FixedSchema::builder().name(name.clone()).size(32).build())
        }
    }
}

impl From<Id> for Value {
    fn from(id: Id) -> Self {
        Value::Fixed(32, id.0.to_vec())
    }
}

impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // write out inner array as one long hex string
        write!(f, "{}", self.hex_string())
    }
}

impl std::fmt::Debug for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hex_string())
    }
}

impl Eq for Id {}

impl std::convert::From<i64> for Id {
    fn from(i: i64) -> Self {
        let bs = i.to_le_bytes();
        let mut id = [0u8; 32];
        id[..8].copy_from_slice(&bs[0..8]);
        Id(id)
    }
}

impl std::convert::From<[u8; 32]> for Id {
    fn from(bs: [u8; 32]) -> Self {
        Id(bs)
    }
}

#[derive(Debug, Error)]
pub enum BeechError {
    #[error("nonzero sqlite3 result code: {}", code)]
    Sqlite3 { code: i32 },
    #[error("done")]
    Done,
    #[error("invalid argument(s)")]
    Args,
    #[error("corrupt database: {0}")]
    Corrupt(String),
    #[error("HTTP result code :{}", code)]
    Http { code: i32 },
    #[error("unknown network failure")]
    Network,
    #[error("schema changed during query processing")]
    SchemaMismatch,
    #[error("no such table")]
    NoSuchTable,
    #[error("serialization error: {0}")]
    Serialization(#[from] apache_avro::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("object not found: {0}")]
    NotFound(String),
}

pub fn err_corrupt(s: impl AsRef<str>) -> BeechError {
    BeechError::Corrupt(s.as_ref().to_string())
}

pub type Result<T> = std::result::Result<T, BeechError>;

#[derive(Debug, Clone, PartialEq)]
pub struct Root {
    pub id: Id,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Page {
    Branch { keys: Vec<Key>, children: Vec<Id>, depth: i32, row_count: i64 },
    Leaf { keys: Vec<Key>, rows: Vec<Row> },
}

impl Page {
    pub fn keys(&self) -> &[Key] {
        match self {
            Page::Branch { keys, .. } => keys,
            Page::Leaf { keys, .. } => keys,
        }
    }
    pub fn is_leaf(&self) -> bool {
        matches!(self, Page::Leaf { .. })
    }
    pub fn last_child(&self) -> usize {
        match self {
            Page::Leaf { rows, .. } => rows.len() - 1,
            Page::Branch { children, .. } => children.len() - 1,
        }
    }
    pub fn depth(&self) -> i32 {
        match self {
            Page::Branch { depth, .. } => *depth,
            Page::Leaf { .. } => 0,  // Leaf pages always have depth 0
        }
    }
    pub fn row_count(&self) -> i64 {
        match self {
            Page::Branch { row_count, .. } => *row_count,
            Page::Leaf { rows, .. } => rows.len() as i64,  // Computed from rows
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Table {
    pub id: Id,
    pub name: String,
    pub root: Option<Id>,
    pub key_scheme: Schema,
    pub row_scheme: Schema,
    pub key_columns: Vec<(usize, Column)>,  
    columns: Vec<Column>,
}
fn read_field_type(f: &RecordField) -> Result<Schema> {
    use apache_avro::Schema::*;
    match &f.schema {
        Union(u) => {
            if let &[Schema::Null, ref t] = u.variants() {
                Ok(t.clone())
            } else {
                Err(err_corrupt("invalid union schema"))
            }
        }
        Null | Boolean | Int | Long | Float | Double | Bytes | String | Fixed(_) | Decimal(_)
        | BigDecimal | Uuid | Date | TimeMillis | TimeMicros | TimestampMillis
        | TimestampMicros | TimestampNanos | LocalTimestampMillis | LocalTimestampMicros
        | LocalTimestampNanos | Duration => Ok(f.schema.clone()),
        _ => Err(err_corrupt(format!("invalid field type: {:?}", f.schema))),
    }
}
impl Table {
    pub fn new(
        id: Id,
        name: String,
        root: Option<Id>,
        key_scheme: Schema,
        row_scheme: Schema,
    ) -> Result<Table> {
        if let (
            Schema::Record(RecordSchema {
                fields: key_fields, ..
            }),
            Schema::Record(RecordSchema {
                fields: row_fields, ..
            }),
        ) = (&key_scheme, &row_scheme)
        {
            // iterate over the fields of the row, and find the corresponding key fields
            let columns: Vec<_> = row_fields
                .iter()
                .map(|field| {
                    let key_index = key_fields
                        .iter()
                        .position(|key_field| key_field.name == field.name);
                    Ok(Column {
                        key_index,
                        name: field.name.clone(),
                        typ: read_field_type(field)?,
                    })
                })
                .collect::<Result<_>>()?;
            let key_columns = key_fields
                .iter()
                .map(|field| {
                    let row_location = columns
                        .iter()
                        .position(|column| column.name == field.name)
                        .unwrap();
                    Ok((
                        row_location,
                        Column {
                            key_index: None,
                            name: field.name.clone(),
                            typ: read_field_type(field)?,
                        },
                    ))
                })
                .collect::<Result<_>>()?;
            Ok(Table {
                id,
                name,
                key_scheme,
                row_scheme,
                root,
                key_columns,
                columns,
            })
        } else {
            Err(err_corrupt("invalid table schema, should be a record"))
        }
    }

    /// Gets a reference to the column at the given index
    pub fn column(&self, index: usize) -> Option<&Column> {
        self.columns.get(index)
    }
    
    /// Gets the key part index for a column, if it's part of the key.
    /// e.g. if row has columns A,B,C,D and key uses B,C then:
    /// column_key_index(1) == Some(0)  // B is the first key part
    /// column_key_index(2) == Some(1)  // C is the second key part
    /// column_key_index(0) == None     // A is not part of the key
    pub fn column_key_index(&self, col_index: usize) -> Option<usize> {
        self.column(col_index).and_then(|col| col.key_index)
    }
    
    pub fn columns(&self) -> &[Column] {
        &self.columns[..]
    }
}

#[derive(Debug, Clone)]
pub struct Transaction {
    pub id: Id,
    pub prev_id: Id,
    pub transaction_time: SystemTime,
    pub tables: HashMap<String, Id>,
}


pub trait NodeSource {
         fn get_root(&self) -> Result<Arc<Root>>;    
         fn get_transaction(&self, transaction_id: &Id) -> Result<Arc<Transaction>>;
         fn get_table(&self, transaction: &Transaction, table_name: &str) -> Result<Arc<Table>>;
         fn get_page(
            &self,
            page_id: &Id,
            key_scheme: &Schema,
            row_scheme: &Schema,
        ) -> Result<Arc<Page>>;    
}

pub trait BackingStore<K> {
    fn get(&self, key: &K) -> Result<Option<Arc<MappedBuffer>>>;

    fn insert(&self, _key: K, _value: Arc<MappedBuffer>) -> Result<()> {
        Ok(())
    }
}