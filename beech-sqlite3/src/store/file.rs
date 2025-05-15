use beech_core::BackingStore;
use beech_core::Result;
use beech_mem::mmap::MappedBuffer;
use std::fs::File;
use std::path::PathBuf;
use std::{fmt::Debug, sync::Arc};

/// A Tier backed by the filesystem, with a pluggable key-to-path mapping.
pub struct FileStore<K> {
    root: PathBuf,
    key_to_path: Box<dyn Fn(&K) -> PathBuf + Send + Sync>,
}

impl<K> FileStore<K> {
    pub fn new<P, F>(root: P, key_to_path: F) -> Self
    where
        P: Into<PathBuf>,
        F: Fn(&K) -> PathBuf + Send + Sync + 'static,
    {
        Self {
            root: root.into(),
            key_to_path: Box::new(key_to_path),
        }
    }

    fn full_path(&self, key: &K) -> PathBuf {
        self.root.join((self.key_to_path)(key))
    }
}

impl<K> BackingStore<K> for FileStore<K>
where
    K: Debug,
{
    fn get(&self, key: &K) -> Result<Option<Arc<MappedBuffer>>> {
        let path = self.full_path(key);

        match MappedBuffer::from_file(&File::open(&path)?) {
            Ok(buf) => Ok(Some(buf)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    fn insert(&self, _key: K, _value: Arc<MappedBuffer>) -> Result<()> {
        unimplemented!()
    }
}
