use super::{BeechError, DOMAIN_SCHEME, GROUND_SCHEME, PAGE_SCHEME, TABLE_SCHEME};

use crate::wire;
use crate::wire::Ground;
use crate::Id;
use crate::{Domain, Page, Table, TableMetadata};
use anyhow::Result;
use apache_avro::types::Value;
use apache_avro::{from_avro_datum, from_value};
use std::cell::RefCell;
use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::Arc;

pub trait Store {
    fn get_domain(&self) -> Result<Arc<Domain>>;
    fn get_table(&self, name: &str) -> Result<Arc<Table>>;
    fn get_page(&self, id: &Id, metadata: &TableMetadata) -> Result<Arc<Page>>;
}

pub struct CachedStore<F>
where
    F: Fn(Option<&Id>, &apache_avro::Schema) -> Result<Value>,
{
    page_cache: RefCell<lru_cache::LruCache<Id, Arc<Page>>>,
    table_cache: RefCell<lru_cache::LruCache<Id, Arc<Table>>>,
    fetch: F,
}

impl<F> CachedStore<F>
where
    F: Fn(Option<&Id>, &apache_avro::Schema) -> Result<Value>,
{
    pub fn new(cache_size: usize, fetcher: F) -> Result<CachedStore<F>> {
        Ok(CachedStore {
            page_cache: RefCell::new(lru_cache::LruCache::new(cache_size)),
            table_cache: RefCell::new(lru_cache::LruCache::new(cache_size)),
            fetch: fetcher,
        })
    }

    fn fetch_ground(&self) -> Result<Ground> {
        let aval = (self.fetch)(None, &GROUND_SCHEME)?;
        Ok(from_value::<Ground>(&aval)?)
    }

    fn get_table_id(&self, name: &str) -> Result<Id> {
        let domain = self.get_domain()?;
        domain
            .as_ref()
            .tables
            .get(name)
            .cloned()
            .ok_or(BeechError::Corrupt.into())
    }

    pub fn get_domain(&self) -> Result<Arc<Domain>> {
        let ground = self.fetch_ground()?;
        let adomain = (self.fetch)(Some(&ground.id), &DOMAIN_SCHEME)?;
        let wire_domain = from_value::<wire::Domain>(&adomain)?;
        let domain = (&wire_domain).try_into()?;
        Ok(Arc::new(domain))
    }

    pub fn get_table(&self, name: &str) -> Result<Arc<Table>> {
        let id = self.get_table_id(name)?;
        if !self.table_cache.borrow_mut().contains_key(&id) {
            let atable = (self.fetch)(Some(&id), &TABLE_SCHEME)?;
            let wire_table = from_value::<wire::Table>(&atable)?;
            let metadata: TableMetadata =
                (wire_table.key_scheme.as_str(), wire_table.record_scheme.as_str()).try_into()?;
            let table: Table = (&wire_table, metadata).try_into()?;
            let arc_table = Arc::new(table);

            self.table_cache.borrow_mut().insert(id.clone(), arc_table.clone());
            return Ok(arc_table);
        }
        self.table_cache
            .borrow_mut()
            .get_mut(&id)
            .map(|a| a.clone())
            .ok_or_else(|| BeechError::NoSuchTable.into())
    }

    pub fn get_page(&self, id: &Id, metadata: &TableMetadata) -> Result<Arc<Page>> {
        if !self.page_cache.borrow_mut().contains_key(id) {
            let apage = (self.fetch)(Some(id), &PAGE_SCHEME)?;
            let wire_page = from_value::<wire::Page>(&apage)?;
            let page: Page = (&wire_page, metadata).try_into()?;
            let arc_page = Arc::new(page);

            self.page_cache.borrow_mut().insert(id.clone(), arc_page.clone());
            return Ok(arc_page);
        }
        self.page_cache
            .borrow_mut()
            .get_mut(id)
            .map(|a| a.clone())
            .ok_or_else(|| BeechError::Corrupt.into())
    }
}

pub fn fetch_file_value(pb: &PathBuf, maybe_id: Option<&Id>, scheme: &apache_avro::Schema) -> Result<Value> {
    let mut pb = pb.clone();
    if let Some(id) = maybe_id {
        pb.push(id.to_string());
        pb.push(".dat");
    } else {
        pb.push("ground");
    }
    let mut f = std::fs::File::open(pb)?;
    Ok(from_avro_datum(scheme, &mut f, None)?)
}

#[cfg(feature = "curl")]
pub fn fetch_http_value(url_base: &url::Url, maybe_id: Option<&Id>, scheme: &apache_avro::Schema) -> Result<Value> {
    let u = if let Some(id) = maybe_id {
        url_base.join((id.to_string() + ".dat").as_str())?
    } else {
        url_base.join("ground")?
    };

    crate::http::get(&u).and_then(|(_, data)| {
        let mut cursor = std::io::Cursor::new(data);
        Ok(from_avro_datum(scheme, &mut cursor, None)?)
    })
}
