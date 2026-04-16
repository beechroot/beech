//! In-memory test fixtures for beech.
//!
//! `MemoryStore` is a paired `MemoryWriter` + `MemoryNodeSource` over a
//! shared `Arc<Mutex<HashMap<String, Vec<u8>>>>`. Bytes written via the
//! writer become readable through the node source — useful for round-trip
//! tests of the write+read pipeline without touching the filesystem.
//!
//! `build_simple_table` is the all-in-one helper: build a tree from a
//! `(row_id, record)` list and hand back a node source plus the resolved
//! `Table` snapshot, ready for cursor or query exercises.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex};

use apache_avro::types::Value;
use beech_core::wire::{decode_page, decode_root, decode_table, decode_transaction};
use beech_core::{
    DomainError, Id, Node, NodeSource, Result, Root, StorageError, Table, TableSchema, Transaction,
};

type Files = Arc<Mutex<HashMap<String, Vec<u8>>>>;

/// Shared in-memory backing store. Construct one, then call [`Self::writer`]
/// and [`Self::node_source`] as many times as needed; they all reference the
/// same underlying map.
#[derive(Clone, Default)]
pub struct MemoryStore {
    files: Files,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn writer(&self) -> MemoryWriter {
        MemoryWriter {
            files: Arc::clone(&self.files),
        }
    }

    pub fn node_source(&self) -> MemoryNodeSource {
        MemoryNodeSource {
            files: Arc::clone(&self.files),
        }
    }

    /// Number of files currently in the store.
    pub fn file_count(&self) -> usize {
        self.files.lock().unwrap().len()
    }

    /// Raw access to a stored file by name (e.g. `"<id>.bch"` or `"root.bch"`).
    pub fn get(&self, name: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(name).cloned()
    }

    /// Raw insert. Useful for tests that want to corrupt the store.
    pub fn insert(&self, name: impl Into<String>, bytes: Vec<u8>) {
        self.files.lock().unwrap().insert(name.into(), bytes);
    }

    /// Remove a file. Useful for tests that simulate missing-file scenarios.
    pub fn remove(&self, name: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().remove(name)
    }
}

pub struct MemoryWriter {
    files: Files,
}

impl beech_write::Writer for MemoryWriter {
    fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>,
    {
        let filename = name.as_ref().to_string_lossy().to_string();
        self.files.lock().unwrap().insert(filename, data.to_vec());
        Ok(())
    }

    fn commit(self) -> std::io::Result<()> {
        Ok(())
    }

    fn abort(self) -> std::io::Result<()> {
        Ok(())
    }

    fn num_to_commit(&self) -> usize {
        self.files.lock().unwrap().len()
    }
}

pub struct MemoryNodeSource {
    files: Files,
}

impl MemoryNodeSource {
    fn get_file_data(&self, id: &Id) -> Result<Vec<u8>> {
        let filename = format!("{id}.bch");
        self.files
            .lock()
            .unwrap()
            .get(&filename)
            .cloned()
            .ok_or_else(|| StorageError::key_not_found("file", id.clone()).into())
    }
}

impl beech_core::NodeSource for MemoryNodeSource {
    fn get_root(&self) -> Result<Arc<Root>> {
        let files = self.files.lock().unwrap();
        if let Some(data) = files.get("root.bch") {
            let mut cursor = Cursor::new(data.clone());
            drop(files);
            Ok(Arc::new(decode_root(&mut cursor)?))
        } else {
            Err(StorageError::key_not_found("root", Default::default()).into())
        }
    }

    fn get_transaction(&self, transaction_id: &Id) -> Result<Arc<Transaction>> {
        let data = self.get_file_data(transaction_id)?;
        let mut cursor = Cursor::new(data);
        Ok(Arc::new(decode_transaction(&mut cursor)?))
    }

    fn get_table(&self, transaction: &Transaction, table_name: &str) -> Result<Arc<Table>> {
        if let Some(table_id) = transaction.tables.get(table_name) {
            let data = self.get_file_data(table_id)?;
            let mut cursor = Cursor::new(data);
            Ok(Arc::new(decode_table(&mut cursor)?))
        } else {
            Err(DomainError::NoSuchTable {
                name: table_name.to_string(),
            }
            .into())
        }
    }

    fn get_page(&self, page_id: &Id, schema: &TableSchema) -> Result<Arc<Node>> {
        let data = self.get_file_data(page_id)?;
        let mut cursor = Cursor::new(data);
        Ok(Arc::new(decode_page(&mut cursor, schema)?))
    }
}

/// Build a fresh prolly tree in memory from `(row_id, record)` pairs.
///
/// Returns the populated `MemoryStore`, a `MemoryNodeSource` view into it,
/// and the resolved `Table` snapshot — ready to feed a `Cursor` or any
/// other code that wants a built table to read against.
///
/// `key_columns` lists row-column indices that participate in the key.
/// `target_page_size` controls the prolly-tree split target.
pub fn build_simple_table(
    table_name: &str,
    rows: Vec<(i64, Value)>,
    key_columns: Vec<usize>,
    target_page_size: usize,
    page_size_stddev: usize,
) -> Result<(MemoryStore, MemoryNodeSource, Arc<Table>)> {
    let store = MemoryStore::new();
    let mut writer = store.writer();
    let (transaction_id, _table_id) = beech_write::write_rows_to_prolly_tree(
        &mut writer,
        table_name.to_string(),
        key_columns,
        rows,
        target_page_size,
        page_size_stddev,
        None,
    )?;
    // Persist the root pointer just like beech-cli's load_csv does.
    let root_bytes = transaction_id.to_string();
    store.insert("root", root_bytes.into_bytes());

    let node_source = store.node_source();
    let transaction = node_source.get_transaction(&transaction_id)?;
    let table = node_source.get_table(&transaction, table_name)?;
    Ok((store, node_source, table))
}
