pub use serde;
#[macro_use]
extern crate serde_derive;

pub mod http;
pub mod query;
pub mod source;
pub mod wire;

use std::collections::HashMap;
use std::fmt;

use avro_rs::types::Value;
use avro_rs::{to_avro_datum, Schema};

use failure::Error;
use failure_derive::Fail;
use lazy_static::*;
use serde_bytes::ByteBuf;
use std::convert::TryFrom;
use std::time::*;

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

#[derive(Debug, Fail)]
pub enum BeechError {
    #[fail(display = "nonzero sqlite3 result code: {}", code)]
    Sqlite3 { code: i32 },
    #[fail(display = "done")]
    Done,
    #[fail(display = "invalid argument(s)")]
    Args,
    #[fail(display = "corrupt database")]
    Corrupt,
    #[fail(display = "HTTP result code :{}", code)]
    Http { code: i32 },
    #[fail(display = "unknown network failure")]
    Network,
    #[fail(display = "schema changed during query processing")]
    SchemaMismatch,
    #[fail(display = "no such table")]
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

pub type Result<T> = std::result::Result<T, Error>;

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

    pub key_scheme: avro_rs::Schema,
    pub key_scheme_text: String,
    pub record_scheme: avro_rs::Schema,
    pub record_scheme_text: String,

    pub key_columns: Vec<Column>,
    columns: Vec<Column>,
    key_column_indexes: Vec<usize>, // mappings from keyparts to columns
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
                if let [serde_json::Value::String(n), serde_json::Value::String(typ)] = v.as_slice()
                {
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

pub fn scheme_from_fields(scheme: &str) -> Result<(serde_json::Value, avro_rs::Schema)> {
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

pub fn read_schema(
    key_scheme: &str,
    record_scheme: &str,
) -> Result<(avro_rs::Schema, avro_rs::Schema, Vec<Column>, Vec<usize>, Vec<Column>)> {
    let (key_json_scheme, key_scheme) = scheme_from_fields(key_scheme)?;
    let (record_json_scheme, record_scheme) = scheme_from_fields(record_scheme)?;

    match (
        &key_json_scheme["fields"],
        &record_json_scheme["fields"],
    ) {
        (serde_json::Value::Array(key_fields), serde_json::Value::Array(record_fields)) => {
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

            let (key_column_lookups, key_columns): (Vec<usize>, Vec<(String, String)>) = key_fields
                .iter()
                .map(|kf| {
                    let (ref key_name, ref value_name) = read_field(kf).unwrap();

                    let lookup = columns[..]
                        .iter()
                        .position(|col| {
                                    key_name == &col.name
                        }).unwrap();
                    (lookup, (key_name.to_string(), value_name.to_string()) )
                })
                .unzip();

            let kc = key_columns.into_iter().map(|(k, t)| Column{ key_index: None, name: k, typ: t }).collect();
            Ok((key_scheme, record_scheme, kc, key_column_lookups, columns))
        }
        _ => Err(BeechError::Corrupt.into()),
    }
}
impl TryFrom<&wire::Domain> for Domain {
    type Error = Error;
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

pub fn extract_keys(
    key_column_indexes: &[usize],
    values: &[Value],
) -> Result<Vec<Vec<Value>>> {
    values
        .iter()
        .map(|row| {
            let keys: Result<Vec<Value>> = key_column_indexes
                .iter()
                .map(|kci| {
                    if let Value::Record(fields) = row {
                        fields
                            .get(*kci)
                            .map(|e| e.1.clone())
                            .ok_or_else(||BeechError::Corrupt.into())
                    } else {
                        Err(BeechError::Corrupt.into())
                    }
                })
                .collect();
            keys
        })
        .collect()
}

impl TryFrom<&wire::Table> for Table {
    type Error = Error;

    fn try_from(wt: &wire::Table) -> Result<Self> {
        let (key_scheme, record_scheme, key_columns, key_column_indexes, columns) =
            read_schema(&wt.key_scheme, &wt.record_scheme)?;
        Ok(Table {
            id: wt.id.clone(),
            root: wt.root.clone(),
            key_scheme,
            key_scheme_text:wt.key_scheme.to_string(),
            record_scheme,
            record_scheme_text:wt.record_scheme.to_string(),
            key_columns,
            columns,
            key_column_indexes,
	    page_size_exp:wt.page_size_exp,
        })
    }
}
impl Table {
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }
    pub fn key_size(&self) -> usize {
        self.key_column_indexes.len()
    }
    pub fn key_part_names(&self) -> impl Iterator<Item=&str> {
        self.key_columns.iter().map(|c| c.name.as_str())
    }
    // gets the index of the key part represented by the given column. e.g.
    // row : A,B,C,D
    // key : B,C
    // column_key_part(2) == 0
    // column_key_part(3) == 1
    pub fn column_key_part(&self, col: usize) -> Option<usize> {
        self.key_column_indexes
            .iter()
            .enumerate()
            .find(|(_, x)| **x == col)
            .map(|(i, _)| i)
    }
    fn decode_leaf_values(&self, page: &wire::Page) -> Result<(Vec<Vec<Value>>, Vec<Value>)> {
	let array_scheme = Schema::Array(Box::new(self.record_scheme.clone()));
        let values = wire::value_array_contents(&array_scheme, &mut &page.rows[..])?;
        let keys = extract_keys(&self.key_column_indexes, &values)?;
        Ok((keys, values))
    }
    fn decode_branch_keys(&self, page: &wire::Page) -> Result<Vec<Value>> {
	let array_scheme = Schema::Array(Box::new(self.key_scheme.clone()));
        Ok(wire::value_array_contents(
            &array_scheme,
            &mut &page.keys[..],
        )?)
    }

    pub fn from_wire_page(&self, wire_page: &wire::Page) -> Result<Page> {
        let is_leaf = wire_page.children.is_empty();
        if is_leaf {
            // leaf
            let (keys, rows) = self.decode_leaf_values(wire_page)?;
            let rows: Vec<Vec<Value>> = rows.iter().map(|row| val_to_vec(row)).collect();

            let rows: Vec<Row> = wire_page.rowids.iter().copied().zip(rows).collect();
            Ok(Page::Leaf { keys, values: rows})
        } else {
            // branch
            let keys = self.decode_branch_keys(wire_page)?;
            Ok(Page::Branch {
                children: wire_page.children.clone(),
                keys: keys.into_iter().map(|k| val_to_vec(&k)).collect(),
            })
        }
    }

    pub fn to_wire_page(&self, page: &Page, page_id: Id) -> Result<wire::Page> {
        match page {
            Page::Leaf { values, .. } => {
                let (rowids, rows): (Vec<_>, Vec<_>) = values.iter().cloned().unzip();
                let values_array = Value::Array(rows.into_iter().map(Value::Array).collect());
                let value_bytes = to_avro_datum(&self.record_scheme, values_array)?;

                Ok(wire::Page {
                    id: page_id,
                    keys: Vec::new(),
                    rows: value_bytes,
                    children: Vec::new(),
                    rowids,
                })
            }
            Page::Branch { keys, children} => {
                let keys_array = Value::Array(keys.iter().cloned().map(Value::Array).collect());

                let key_bytes = to_avro_datum(&self.key_scheme, keys_array)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use avro_rs::{from_avro_datum, from_value, to_avro_datum, to_value};

    #[test]
    fn serialize_round_trip() {
        let mut page: wire::Page = Default::default();
        page.id = 99u64.into();
        page.rowids = vec![3, 4];
        let page_bytes = to_avro_datum(&PAGE_SCHEME, to_value(page).unwrap()).unwrap();
        let rehy_page: wire::Page =
            from_value(&from_avro_datum(&PAGE_SCHEME, &mut &page_bytes[..], None).unwrap())
                .unwrap();
        assert_eq!(rehy_page.id, 99.into());
        assert_eq!(rehy_page.rowids, [3, 4]);

        let mut table: wire::Table = Default::default();
        table.id = 99.into();
        let table_bytes = to_avro_datum(&TABLE_SCHEME, to_value(table).unwrap()).unwrap();
        let rehy_table: wire::Table =
            from_value(&from_avro_datum(&TABLE_SCHEME, &mut &table_bytes[..], None).unwrap())
                .unwrap();
        assert_eq!(rehy_table.id, 99.into());

        let mut domain: wire::Domain = Default::default();
        domain.id = 99.into();
        let domain_bytes = to_avro_datum(&DOMAIN_SCHEME, to_value(domain).unwrap()).unwrap();
        let rehy_domain: wire::Domain =
            from_value(&from_avro_datum(&DOMAIN_SCHEME, &mut &domain_bytes[..], None).unwrap())
                .unwrap();
        assert_eq!(rehy_domain.id, 99.into());
    }
}
