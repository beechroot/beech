use beech_core::{BackingStore, BeechError, Id, Result};
use beech_mem::mmap::MappedBuffer;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn file_name(id: &Id) -> String {
    format!("{id}.bch")
}
pub struct Writer {
    directory: PathBuf,
    written_files: Vec<PathBuf>,
}
impl Writer {
    pub fn new(directory: PathBuf) -> Self {
        Self {
            directory,
            written_files: vec![],
        }
    }

    fn do_abort(&mut self) -> std::io::Result<()> {
        let mut result = Ok(());
        for file in &self.written_files {
            let r = std::fs::remove_file(file);
            result = result.and(r);
        }
        result
    }
}
impl beech_write::Writer for Writer {
    fn commit(mut self) -> std::io::Result<()> {
        self.written_files.clear();
        Ok(())
    }
    #[allow(dead_code)]
    fn abort(mut self) -> std::io::Result<()> {
        self.do_abort()
    }
    fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>,
    {
        let file = self.directory.join(name);
        let mut f = std::fs::File::create(&file)?;
        f.write_all(data)?;
        self.written_files.push(file);
        Ok(())
    }

    fn num_to_commit(&self) -> usize {
        self.written_files.len()
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        let _ = self.do_abort();
    }
}

pub struct FileBackingStore {
    directory: PathBuf,
}

impl FileBackingStore {
    pub fn new(directory: PathBuf) -> Self {
        Self { directory }
    }

    fn file_path(&self, id: &Id) -> PathBuf {
        self.directory.join(file_name(id))
    }
}

impl BackingStore<Id> for FileBackingStore {
    fn get(&self, id: &Id) -> Result<Option<Arc<MappedBuffer>>> {
        let path = self.file_path(id);
        match std::fs::File::open(&path) {
            Ok(file) => match MappedBuffer::from_file(&file) {
                Ok(buffer) => Ok(Some(buffer)),
                Err(e) => Err(BeechError::Corrupt(format!(
                    "Failed to map file {}: {}",
                    path.display(),
                    e
                ))),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(BeechError::Corrupt(format!(
                "Failed to open file {}: {}",
                path.display(),
                e
            ))),
        }
    }
}
