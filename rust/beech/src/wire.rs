use super::*;
use std::collections::HashMap;
use std::mem::size_of;

use avro_rs::types::Value;
use avro_rs::{from_avro_datum, from_value, Schema};

use serde_derive::{Deserialize, Serialize};
use std::io;

#[derive(Clone, Debug, Deserialize, Serialize, Default, PartialEq)]
pub struct Ground {
    pub id: Id,
    pub base: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Domain {
    pub id: Id,
    pub prev_id: Id,
    pub transaction_time: i64,
    pub tables: HashMap<String, Id>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Table {
    pub id: Id,
    pub name: String,
    pub timestamp: i64,
    pub key_scheme: String,
    pub record_scheme: String,
    pub root: Option<Id>,
    pub page_size_exp: u8,
}

#[derive(Clone, Debug, Deserialize, Serialize, Default)]
pub struct Page {
    pub id: Id,
    #[serde(with = "serde_bytes")]
    pub keys: Vec<u8>,
    pub rowids: Vec<i64>,
    #[serde(with = "serde_bytes")]
    pub rows: Vec<u8>,
    pub children: Vec<Id>,
}

impl Domain {
    pub fn read<R: io::Read>(r: &mut R, scheme: &avro_rs::Schema) -> Result<Domain> {
        let next_result = avro_rs::from_avro_datum(scheme, r, None)?;
        Ok(from_value::<wire::Domain>(&next_result)?)
    }
}

impl Table {
    pub fn read<R: io::Read>(r: &mut R, scheme: &avro_rs::Schema) -> Result<Table> {
        let next_result = avro_rs::from_avro_datum(scheme, r, None)?;
        Ok(from_value::<wire::Table>(&next_result)?)
    }
    pub fn data_size(&self) -> usize {
        self.name.len() + self.key_scheme.len() + self.record_scheme.len()
    }
}

pub fn value_array_contents<R: io::Read>(scheme: &Schema, rd: &mut R) -> Result<Vec<Value>> {
    let val = from_avro_datum(scheme, rd, None)?;
    if let Value::Array(arr) = val {
        Ok(arr)
    } else {
        Err(BeechError::Corrupt.into())
    }
}

impl Page {
    pub fn read<R: io::Read>(r: &mut R, scheme: &avro_rs::Schema) -> Result<Page> {
        let next_result = avro_rs::from_avro_datum(scheme, r, None)?;
        Ok(from_value::<wire::Page>(&next_result)?)
    }

    pub fn data_size(&self) -> usize {
        self.keys.len()
            + (self.rowids.len() * size_of::<i64>())
            + self.rows.len()
            + (self.children.len() * size_of::<Id>())
    }
}
