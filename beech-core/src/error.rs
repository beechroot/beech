use crate::{Id, Key};
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("mmap failure at {path}: {source}")]
    Mmap {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{key_kind} not found in store: {key}")]
    KeyNotFound { key_kind: &'static str, key: Id },
}

impl StorageError {
    pub fn key_not_found(key_kind: &'static str, key: Id) -> Self {
        StorageError::KeyNotFound { key_kind, key }
    }
}

#[derive(Debug, Error)]
pub enum WireError {
    #[error("avro error: {0}")]
    Avro(#[from] apache_avro::Error),
    #[error("malformed wire data: {0}")]
    Malformed(&'static str),
    #[error("unexpected type: expected {expected}, got {got}")]
    UnexpectedType {
        expected: &'static str,
        got: &'static str,
    },
    #[error("invalid hex: {0}")]
    InvalidHex(String),
    #[error("invalid utf-8: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("truncated wire data")]
    Truncated,
}

#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("schema mismatch: {0}")]
    Mismatch(String),
    #[error("unsupported field type: {name} ({schema})")]
    UnsupportedFieldType { name: String, schema: String },
    #[error("invalid union schema: {0}")]
    InvalidUnion(String),
    #[error("missing key column: {name}")]
    MissingKeyColumn { name: String },
    #[error("arity mismatch: expected {expected}, got {got}")]
    ArityMismatch { expected: usize, got: usize },
}

impl SchemaError {
    pub fn unsupported_field(name: &str, value: &apache_avro::types::Value) -> Self {
        SchemaError::UnsupportedFieldType {
            name: name.to_string(),
            schema: format!("{value:?}"),
        }
    }
}

#[derive(Debug, Error)]
pub enum DomainError {
    #[error("duplicate key: {key:?}")]
    DuplicateKey { key: Key },
    #[error("key not found: {key:?}")]
    KeyNotFound { key: Key },
    #[error("no such table: {name}")]
    NoSuchTable { name: String },
    #[error("invalid argument(s): {0}")]
    InvalidArgs(String),
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("cursor stack empty")]
    EmptyStack,
    #[error("child index out of bounds: index {index}, len {len}")]
    ChildIndexOutOfBounds { index: usize, len: usize },
    #[error("unexpected page type: expected {expected}, got {got}")]
    UnexpectedPageType {
        expected: &'static str,
        got: &'static str,
    },
    #[error("tree invariant violated: {0}")]
    Malformed(&'static str),
}

#[derive(Debug, Error)]
pub enum BeechError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Wire(#[from] WireError),
    #[error(transparent)]
    Schema(#[from] SchemaError),
    #[error(transparent)]
    Domain(#[from] DomainError),
    #[error(transparent)]
    Query(#[from] QueryError),
}

impl From<std::io::Error> for BeechError {
    fn from(e: std::io::Error) -> Self {
        BeechError::Storage(StorageError::Io(e))
    }
}

impl From<apache_avro::Error> for BeechError {
    fn from(e: apache_avro::Error) -> Self {
        BeechError::Wire(WireError::Avro(e))
    }
}

impl From<std::str::Utf8Error> for BeechError {
    fn from(e: std::str::Utf8Error) -> Self {
        BeechError::Wire(WireError::InvalidUtf8(e))
    }
}

pub type Result<T> = std::result::Result<T, BeechError>;
