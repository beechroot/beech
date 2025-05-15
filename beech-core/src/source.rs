use apache_avro::Schema;
use super::Result;
use super::{
    BackingStore, BeechError, Id, NodeSource, Page, Root, Table, Transaction, err_corrupt,
    wire::{decode_page, decode_root, decode_table, decode_transaction},
};
use std::{io::Cursor, sync::Arc};

pub struct LocalFile<S>
where
    S: BackingStore<Id>,
{
    store: S,
}
impl<S> LocalFile<S>
where
    S: BackingStore<Id>,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    fn get_table_by_id(&self, table_id: &Id) -> Result<Arc<Table>> {
        let Some(table_bytes) = self.store.get(table_id)? else {
            return Err(BeechError::NotFound(format!("table: {table_id}")));
        };
        let mut reader = Cursor::new(&*table_bytes);
        let table = decode_table(&mut reader)?;
        Ok(Arc::new(table))
    }
}
impl<S> NodeSource for LocalFile<S>
where
    S: BackingStore<Id>,
{
    fn get_root(&self) -> Result<Arc<Root>> {
        let Some(root_bytes) = self.store.get(&Default::default())? else {
            return Err(BeechError::NotFound("root".to_string()));
        };
        let mut reader = Cursor::new(&*root_bytes);
        let root = decode_root(&mut reader).map_err(|e| err_corrupt(e.to_string()))?;
        Ok(Arc::new(root))
    }

    fn get_transaction(&self, transaction_id: &Id) -> Result<Arc<Transaction>> {
        let Some(domain_bytes) = self.store.get(transaction_id)? else {
            return Err(BeechError::NotFound(format!("transaction: {transaction_id}")));
        };
        let mut reader = Cursor::new(&*domain_bytes);
        let transaction =
            decode_transaction(&mut reader).map_err(|e| err_corrupt(e.to_string()))?;
        Ok(Arc::new(transaction))
    }
    fn get_table(&self, transaction: &Transaction, table_name: &str) -> Result<Arc<Table>> {
        let table_id = transaction
            .tables
            .get(table_name)
            .ok_or(BeechError::NoSuchTable)?;
        self.get_table_by_id(table_id)
            .map_err(|e| err_corrupt(e.to_string()))
    }
    fn get_page(
        &self,
        page_id: &Id,
        key_scheme: &Schema,
        row_scheme: &Schema,
    ) -> Result<Arc<Page>> {
        let Some(page_bytes) = self.store.get(page_id)? else {
            return Err(BeechError::NotFound(format!("page: {page_id}")));
        };
        let mut reader = Cursor::new(&*page_bytes);
        let page = decode_page(&mut reader, key_scheme, row_scheme)
            .map_err(|e| err_corrupt(e.to_string()))?;
        Ok(Arc::new(page))
    }
}
