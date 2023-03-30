use super::{BeechError, Result, DOMAIN_SCHEME, GROUND_SCHEME, PAGE_SCHEME, TABLE_SCHEME};

use crate::wire;
use crate::wire::Ground;
use crate::Id;
use crate::{Domain, Page, Table, TableMetadata};
use avro_rs::{from_avro_datum, from_value};
use log::info;
#[cfg(feature = "curl")]
use std::convert::TryFrom;
use std::convert::TryInto;
use std::time::{Duration, Instant};

enum Cacheable {
    Table(Table),
    Page(Page),
}

impl Cacheable {
    fn as_table(&self) -> Option<&Table> {
        if let Cacheable::Table(t) = self {
            Some(t)
        } else {
            None
        }
    }
    fn as_page(&self) -> Option<&Page> {
        if let Cacheable::Page(p) = self {
            Some(p)
        } else {
            None
        }
    }
}

impl lru_cache::Sizer for Cacheable {
    fn size(&self) -> usize {
        //TODO:
        1024
    }
}

pub trait ByteFetcher {
    fn get_reader<'a>(&'a self, file_name: Option<&str>) -> Result<Box<dyn std::io::Read + 'a>>;
}

pub trait Store {
    fn get_domain(&mut self) -> Result<(&Domain, &str)>;
    fn get_table(&mut self, name: &str) -> Result<&Table>;
    fn get_page(&mut self, t: &Table, id: &Id) -> Result<&Page>;
}

pub struct CachedStore<F: ByteFetcher> {
    cache: lru_cache::LruCache<Id, Cacheable>,
    fetch_interval: Option<Duration>,
    cached_ground_data: Option<(Instant, Ground, Domain)>,
    fetcher: F,
}

impl<F: ByteFetcher> CachedStore<F> {
    pub fn new(cache_size: usize, fetch_interval: Option<Duration>, fetcher: F) -> Result<CachedStore<F>> {
        Ok(CachedStore {
            cache: lru_cache::LruCache::new(cache_size),
            fetch_interval,
            cached_ground_data: None,
            fetcher,
        })
    }

    pub fn into_fetcher(self) -> F {
        self.fetcher
    }

    fn maybe_fetch_new_ground(&mut self, now: Instant) -> Result<Option<Ground>> {
        let old_ground = match (self.cached_ground_data.as_ref(), self.fetch_interval) {
            (None, _) => None,
            (Some((last, ground, _)), Some(intv)) => {
                if now < *last + intv {
                    return Ok(None);
                } else {
                    Some(ground)
                }
            }
            (_, None) => return Ok(None),
        };

        let mut r = self.fetcher.get_reader(None)?;
        let latest_ground = from_value::<Ground>(&from_avro_datum(&GROUND_SCHEME, &mut r, None)?)?;
        if Some(&latest_ground) == old_ground {
            Ok(None)
        } else {
            Ok(Some(latest_ground))
        }
    }

    pub fn get_table_id(&mut self, name: &str) -> Result<Id> {
        let (domain, _) = self.get_domain()?;
        domain
            .tables
            .get(name)
            .cloned()
            .ok_or_else(|| -> failure::Error { BeechError::Corrupt.into() })
    }
}
impl<F: ByteFetcher> Store for CachedStore<F> {
    fn get_domain(&mut self) -> Result<(&Domain, &str)> {
        let now = Instant::now();
        let maybe_new_ground = self.maybe_fetch_new_ground(now)?;
        if let Some(new_ground) = maybe_new_ground {
            let dat_file = format!("{}.dat", new_ground.id);
            info!("{}: fetching", dat_file);
            assert!(dat_file != ".dat");
            let mut domain_r = self.fetcher.get_reader(Some(&dat_file))?;
            let wire_domain = wire::Domain::read(&mut domain_r, &DOMAIN_SCHEME)?;
            let domain = (&wire_domain).try_into()?;
            self.cached_ground_data = Some((now, new_ground, domain));
        }
        if let Some(cached_data) = self.cached_ground_data.as_mut() {
            cached_data.0 = now;
            Ok((&cached_data.2, cached_data.1.base.as_str()))
        } else {
            Err(BeechError::Corrupt.into())
        }
    }

    fn get_table(&mut self, name: &str) -> Result<&Table> {
        let id = self.get_table_id(name)?;
        let cached = self.cache.contains_key(&id);
        if !cached {
            let dat_file = format!("{}.dat", &id);
            info!("{}: fetching", dat_file);
            assert!(dat_file != ".dat");
            let mut r = self.fetcher.get_reader(Some(&dat_file))?;
            let wire_table = wire::Table::read(&mut r, &TABLE_SCHEME)?;
            let metadata: TableMetadata =
                (wire_table.key_scheme.as_str(), wire_table.record_scheme.as_str()).try_into()?;
            let table: Table = (&wire_table, metadata).try_into()?;

            self.cache.insert(id.clone(), Cacheable::Table(table));
        }
        self.cache
            .get_mut(&id)
            .and_then(|x| x.as_table())
            .ok_or_else(|| BeechError::NoSuchTable.into())
    }
    fn get_page(&mut self, t: &Table, id: &Id) -> Result<&Page> {
        if !self.cache.contains_key(id) {
            let dat_file = format!("{}.dat", id);
            info!("{}: fetching", dat_file);
            assert!(dat_file != ".dat");
            let mut r = self.fetcher.get_reader(Some(&dat_file))?;
            let wire_page = wire::Page::read(&mut r, &PAGE_SCHEME)?;
            let page: Page = (&wire_page, &t.metadata).try_into()?;

            self.cache.insert(id.clone(), Cacheable::Page(page));
        }
        self.cache
            .get_mut(id)
            .and_then(|x| x.as_page())
            .ok_or_else(|| BeechError::Corrupt.into())
    }
}

pub struct FileFetcher(std::path::PathBuf);

impl<S> From<S> for FileFetcher
where
    S: ToString,
{
    fn from(s: S) -> Self {
        FileFetcher(std::path::PathBuf::from(s.to_string()))
    }
}

impl ByteFetcher for FileFetcher {
    fn get_reader(&self, file_name: Option<&str>) -> Result<Box<dyn std::io::Read>> {
        let mut pb = self.0.clone();
        if let Some(name) = file_name {
            pb.push(name)
        } else {
            pb.push("ground")
        }
        let f = std::fs::File::open(pb)?;
        Ok(Box::new(f) as Box<dyn std::io::Read>)
    }
}

#[cfg(feature = "curl")]
pub struct HttpFetcher(url::Url);

#[cfg(feature = "curl")]
impl TryFrom<&str> for HttpFetcher {
    type Error = failure::Error;
    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        let url = url::Url::parse(s)?;
        Ok(HttpFetcher(url))
    }
}

#[cfg(feature = "curl")]
impl ByteFetcher for HttpFetcher {
    fn get_reader(&self, file_name: Option<&str>) -> Result<Box<dyn std::io::Read>> {
        let u = file_name
            .map(|n| self.0.join(n))
            .unwrap_or_else(|| self.0.join("ground"))?;
        crate::http::get(&u).map(|(_, data)| Box::new(std::io::Cursor::new(data)) as Box<dyn std::io::Read>)
    }
}
