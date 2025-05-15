use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;

use apache_avro::Schema;
use beech_core::wire::{decode_page, decode_root, decode_table, decode_transaction};
use beech_core::{Id, Page, Result, Root, Table, Transaction};

/// Memory-backed writer for testing that doesn't write to filesystem
pub struct Writer {
    pub files: HashMap<String, Vec<u8>>,
}

impl Writer {
    pub fn new() -> Self {
        Self {
            files: HashMap::new(),
        }
    }

    pub fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>,
    {
        let filename = name.as_ref().to_string_lossy().to_string();
        self.files.insert(filename, data.to_vec());
        Ok(())
    }

    pub fn get_file(&self, name: &str) -> Option<&[u8]> {
        self.files.get(name).map(|v| v.as_slice())
    }
}

/// Memory-backed NodeSource implementation for testing
pub struct NodeSource {
    files: HashMap<String, Vec<u8>>,
}

impl NodeSource {
    pub fn new(files: HashMap<String, Vec<u8>>) -> Self {
        Self { files }
    }

    fn get_file_data(&self, id: &Id) -> Result<Vec<u8>> {
        let filename = format!("{id}.bch");
        self.files
            .get(&filename)
            .cloned()
            .ok_or_else(|| beech_core::err_corrupt("File not found"))
    }
}

impl beech_core::NodeSource for NodeSource {
    fn get_root(&self) -> Result<Arc<Root>> {
        if let Some(data) = self.files.get("root.bch") {
            let mut cursor = Cursor::new(data);
            Ok(Arc::new(decode_root(&mut cursor)?))
        } else {
            Err(beech_core::err_corrupt("Root file not found"))
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
            Err(beech_core::err_corrupt("Table not found in transaction"))
        }
    }

    fn get_page(
        &self,
        page_id: &Id,
        key_scheme: &Schema,
        row_scheme: &Schema,
    ) -> Result<Arc<Page>> {
        let data = self.get_file_data(page_id)?;
        let mut cursor = Cursor::new(data);
        Ok(Arc::new(decode_page(&mut cursor, key_scheme, row_scheme)?))
    }
}

impl beech_write::Writer for Writer {
    fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>,
    {
        self.write(name, data)
    }
    
    fn commit(self) -> std::io::Result<()> {
        // For memory writer, commit is a no-op since everything is already in memory
        Ok(())
    }
    
    fn abort(self) -> std::io::Result<()> {
        // For memory writer, abort just drops the data
        Ok(())
    }
    
    fn num_to_commit(&self) -> usize {
        self.files.len()
    }
}

