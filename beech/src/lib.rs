use lru_cache::Sizer;
pub use serde;
#[macro_use]
extern crate serde_derive;

pub mod http;
pub mod query;
pub mod source;
pub mod wire;

use std::collections::HashMap;
use std::fmt;

use apache_avro::types::Value;
use apache_avro::{to_avro_datum, Schema};

use lazy_static::*;
use serde_bytes::ByteBuf;
use std::convert::{TryFrom, TryInto};
use std::time::*;

use anyhow::Result;
use thiserror::Error;

#[derive(Serialize, Deserialize, std::hash::Hash, Default, Clone, PartialEq)]
pub struct Id(ByteBuf);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for e in self.0.iter() {
            write!(f, "{:02X}", e)?;
        }
        Ok(())
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/*impl std::clone::Clone for Id {
    fn clone(&self) -> Self {
         Id(self.0.clone())
    }
}*/

impl Eq for Id {}
impl std::convert::From<&[u8]> for Id {
    fn from(v: &[u8]) -> Self {
        Id(v.to_vec().into())
    }
}

impl std::convert::From<Vec<u8>> for Id {
    fn from(v: Vec<u8>) -> Self {
        (&v[..]).into()
    }
}
impl std::convert::Into<Vec<u8>> for Id {
    fn into(self) -> Vec<u8> {
        self.0.to_vec()
    }
}
impl std::convert::From<[u8; 20]> for Id {
    fn from(arr: [u8; 20]) -> Self {
        Id(arr.to_vec().into())
    }
}

impl std::convert::From<u64> for Id {
    fn from(u: u64) -> Self {
        Id(u.to_be_bytes().to_vec().into())
    }
}

impl std::convert::AsRef<[u8]> for Id {
    fn as_ref(&self) -> &[u8] {
        &self.0[..]
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
    #[error("corrupt database")]
    Corrupt,
    #[error("HTTP result code :{}", code)]
    Http { code: i32 },
    #[error("unknown network failure")]
    Network,
    #[error("schema changed during query processing")]
    SchemaMismatch,
    #[error("no such table")]
    NoSuchTable,
}

lazy_static! {
    pub static ref DOMAIN_SCHEME: Schema =
        Schema::parse(&serde_json::from_str(include_str!("domainscheme")).unwrap()).unwrap();
    pub static ref PAGE_SCHEME: Schema =
        Schema::parse(&serde_json::from_str(include_str!("pagescheme")).unwrap()).unwrap();
    pub static ref TABLE_SCHEME: Schema =
        Schema::parse(&serde_json::from_str(include_str!("tablescheme")).unwrap()).unwrap();
    pub static ref GROUND_SCHEME: Schema =
        Schema::parse(&serde_json::from_str(include_str!("groundscheme")).unwrap()).unwrap();
}

pub type Row = (i64, Vec<Value>);
pub type Key = Vec<Value>; //column number and value

#[derive(Debug, Clone)]
pub struct Column {
    pub key_index: Option<usize>,
    pub name: String,
    pub typ: String,
}

#[derive(Debug, Clone)]
pub struct Domain {
    pub id: Id,
    pub prev_id: Id,
    pub transaction_time: SystemTime,
    pub tables: HashMap<String, Id>,
}

#[derive(Debug, Clone)]
pub enum Page {
    Branch { keys: Vec<Key>, children: Vec<Id> },
    Leaf { keys: Vec<Key>, values: Vec<Row> },
}

#[derive(Debug, Clone)]
pub struct Table {
    pub id: Id,
    pub root: Option<Id>,
    pub page_size_exp: u8,
    pub metadata: TableMetadata,
}

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub key_scheme: apache_avro::Schema,
    pub key_scheme_text: String,
    pub record_scheme: apache_avro::Schema,
    pub record_scheme_text: String,
    pub key_columns: Vec<(usize, Column)>,
    columns: Vec<Column>,
}

impl Page {
    pub fn is_leaf(&self) -> bool {
        matches!(self, Page::Leaf { .. })
    }
    pub fn last_child(&self) -> usize {
        match self {
            Page::Leaf { values, .. } => values.len() - 1,
            Page::Branch { children, .. } => children.len() - 1,
        }
    }
    pub fn column(&self, row: usize, col: usize) -> Option<&Value> {
        if let Page::Leaf { values, .. } = self {
            values.get(row).and_then(|r| r.1.get(col))
        } else {
            None
        }
    }
    pub fn rowid(&self, row: usize) -> Option<i64> {
        if let Page::Leaf { values, .. } = self {
            values.get(row).map(|r| r.0)
        } else {
            None
        }
    }
    pub fn keys(&self) -> &[Key] {
        match self {
            Page::Leaf { keys, .. } => &keys[..],
            Page::Branch { keys, .. } => &keys[..],
        }
    }
    pub fn key_part(&self, row: usize, index: usize) -> Option<&Value> {
        let keys = match self {
            Page::Leaf { keys, .. } => keys,
            Page::Branch { keys, .. } => keys,
        };
        keys.get(row).and_then(|k| k.get(index))
    }
}

fn read_field(v: &serde_json::Value) -> Option<(String, String)> {
    if let serde_json::Value::Object(ref o) = v {
        match (&o["name"], &o["type"]) {
            (serde_json::Value::String(name), serde_json::Value::String(typ)) => {
                Some((name.to_string(), typ.to_string()))
            }

            (serde_json::Value::String(name), serde_json::Value::Array(v)) => {
                if let [serde_json::Value::String(n), serde_json::Value::String(typ)] = v.as_slice() {
                    if n == "null" {
                        Some((name.to_string(), typ.to_string()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }

            _ => None,
        }
    } else {
        None
    }
}

pub fn scheme_from_fields(scheme: &str) -> Result<(serde_json::Value, apache_avro::Schema)> {
    let str_scheme = format!(
        r#"
{{
    "type": "record",
    "name": "page",
    "fields": {}
}}
"#,
        scheme
    );
    let json_scheme: serde_json::Value = serde_json::from_str(&str_scheme)?;
    let scheme = Schema::parse(&json_scheme)?;

    Ok((json_scheme, scheme))
}

impl TryFrom<(&str, &str)> for TableMetadata {
    type Error = anyhow::Error;

    fn try_from((key_scheme_text, record_scheme_text): (&str, &str)) -> std::result::Result<Self, Self::Error> {
        let (key_json_scheme, key_scheme) = scheme_from_fields(key_scheme_text)?;
        let (record_json_scheme, record_scheme) = scheme_from_fields(record_scheme_text)?;

        if let (serde_json::Value::Array(key_fields), serde_json::Value::Array(record_fields)) =
            (&key_json_scheme["fields"], &record_json_scheme["fields"])
        {
            {
                let columns: Vec<Column> = record_fields
                    .iter()
                    .filter_map(|x| {
                        read_field(x).map(|x| {
                            let (ref name, ref typ) = x;
                            Column {
                                key_index: key_fields.iter().position(|kf| {
                                    read_field(kf)
                                        .map(|(key_field_name, _)| &key_field_name == name)
                                        .unwrap_or(false)
                                }),
                                name: name.to_string(),
                                typ: typ.to_string(),
                            }
                        })
                    })
                    .collect();

                let key_columns: Vec<_> = key_fields
                    .iter()
                    .map(|kf| {
                        let (ref key_name, ref type_name) = read_field(kf).unwrap();

                        let lookup = columns[..].iter().position(|col| key_name == &col.name).unwrap();
                        (
                            lookup,
                            Column {
                                key_index: None,
                                name: key_name.to_string(),
                                typ: type_name.to_string(),
                            },
                        )
                    })
                    .collect();

                Ok(TableMetadata {
                    key_scheme,
                    key_scheme_text: key_scheme_text.to_string(),
                    record_scheme,
                    record_scheme_text: record_scheme_text.to_string(),
                    key_columns,
                    columns,
                })
            }
        } else {
            Err(BeechError::Corrupt.into())
        }
    }
}

impl TryFrom<&wire::Domain> for Domain {
    type Error = anyhow::Error;
    fn try_from(wd: &wire::Domain) -> std::result::Result<Self, Self::Error> {
        let timestamp = UNIX_EPOCH + Duration::from_micros(wd.transaction_time as u64);
        Ok(Domain {
            id: wd.id.clone(),
            prev_id: wd.prev_id.clone(),
            transaction_time: timestamp,
            tables: wd.tables.clone(),
        })
    }
}
impl Domain {
    pub fn data_size(&self) -> usize {
        std::mem::size_of::<Domain>() + (self.tables.len() * 64)
    }
}

fn val_to_vec(val: &Value) -> Vec<Value> {
    let mut v: Vec<Value> = Vec::new();
    if let Value::Record(fields) = val {
        for (_, val) in fields {
            v.push(val.clone());
        }
    };
    v
}

pub fn rehydrate_keys(key_columns: &[(usize, Column)], values: &[Value]) -> Result<Vec<Vec<Value>>> {
    values
        .iter()
        .map(|row| {
            let keys: Result<Vec<Value>> = key_columns
                .iter()
                .map(|(kci, _)| {
                    if let Value::Record(fields) = row {
                        fields
                            .get(*kci)
                            .map(|e| e.1.clone())
                            .ok_or_else(|| BeechError::Corrupt.into())
                    } else {
                        Err(BeechError::Corrupt.into())
                    }
                })
                .collect();
            keys
        })
        .collect()
}

impl TryFrom<(&wire::Table, TableMetadata)> for Table {
    type Error = anyhow::Error;

    fn try_from((wt, metadata): (&wire::Table, TableMetadata)) -> Result<Self> {
        Ok(Table {
            id: wt.id.clone(),
            root: wt.root.clone(),
            page_size_exp: wt.page_size_exp,
            metadata,
        })
    }
}
impl TableMetadata {
    // gets the index of the key part represented by the given column. e.g.
    // row : A,B,C,D
    // key : B,C
    // column_key_part(2) == 0
    // column_key_part(3) == 1
    pub fn column_key_part(&self, col: usize) -> Option<usize> {
        self.key_columns
            .iter()
            .enumerate()
            .find(|(_, (x, _))| *x == col)
            .map(|(i, _)| i)
    }
}
impl TryInto<wire::Page> for (&Page, Id, &TableMetadata) {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<wire::Page> {
        let (page, page_id, metadata) = self;
        match page {
            Page::Leaf { values, .. } => {
                let (rowids, rows): (Vec<_>, Vec<_>) = values.iter().cloned().unzip();
                let values_array = Value::Array(rows.into_iter().map(Value::Array).collect());
                let value_bytes = to_avro_datum(&metadata.record_scheme, values_array)?;

                Ok(wire::Page {
                    id: page_id,
                    keys: Vec::new(),
                    rows: value_bytes,
                    children: Vec::new(),
                    rowids,
                })
            }
            Page::Branch { keys, children } => {
                let keys_array = Value::Array(keys.iter().cloned().map(Value::Array).collect());

                let key_bytes = to_avro_datum(&metadata.key_scheme, keys_array)?;
                Ok(wire::Page {
                    id: page_id,
                    keys: key_bytes,
                    rows: Vec::new(),
                    children: children.clone(),
                    rowids: Vec::new(),
                })
            }
        }
    }
}

impl TryFrom<(&wire::Page, &TableMetadata)> for Page {
    type Error = anyhow::Error;

    fn try_from((wire_page, metadata): (&wire::Page, &TableMetadata)) -> Result<Self> {
        let is_leaf = wire_page.children.is_empty();
        if is_leaf {
            // leaf
            let array_scheme = Schema::Array(Box::new(metadata.record_scheme.clone()));
            let values = wire::value_array_contents(&array_scheme, &mut &wire_page.rows[..])?;
            let keys = rehydrate_keys(&metadata.key_columns, &values)?;

            let rows: Vec<Vec<Value>> = values.iter().map(|row| val_to_vec(row)).collect();

            let rows: Vec<Row> = wire_page.rowids.iter().copied().zip(rows).collect();
            Ok(Page::Leaf { keys, values: rows })
        } else {
            // branch
            let array_scheme = Schema::Array(Box::new(metadata.key_scheme.clone()));
            let keys = wire::value_array_contents(&array_scheme, &mut &wire_page.keys[..])?;
            Ok(Page::Branch {
                children: wire_page.children.clone(),
                keys: keys.into_iter().map(|k| val_to_vec(&k)).collect(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value};

    #[test]
    fn serialize_round_trip() {
        let mut page: wire::Page = Default::default();
        page.id = 99u64.into();
        page.rowids = vec![3, 4];
        let page_bytes = to_avro_datum(&PAGE_SCHEME, to_value(page).unwrap()).unwrap();
        let rehy_page: wire::Page =
            from_value(&from_avro_datum(&PAGE_SCHEME, &mut &page_bytes[..], None).unwrap()).unwrap();
        assert_eq!(rehy_page.id, 99.into());
        assert_eq!(rehy_page.rowids, [3, 4]);

        let mut table: wire::Table = Default::default();
        table.id = 99.into();
        let table_bytes = to_avro_datum(&TABLE_SCHEME, to_value(table).unwrap()).unwrap();
        let rehy_table: wire::Table =
            from_value(&from_avro_datum(&TABLE_SCHEME, &mut &table_bytes[..], None).unwrap()).unwrap();
        assert_eq!(rehy_table.id, 99.into());

        let mut domain: wire::Domain = Default::default();
        domain.id = 99.into();
        let domain_bytes = to_avro_datum(&DOMAIN_SCHEME, to_value(domain).unwrap()).unwrap();
        let rehy_domain: wire::Domain =
            from_value(&from_avro_datum(&DOMAIN_SCHEME, &mut &domain_bytes[..], None).unwrap()).unwrap();
        assert_eq!(rehy_domain.id, 99.into());
    }
}

impl Sizer for Page {
    fn size(&self) -> usize {
        match self {
            Page::Leaf { values, .. } => 1024,
            Page::Branch { keys, .. } => 1024,
        }
    }
}

impl Sizer for Table {
    fn size(&self) -> usize {
        1024
    }
}
