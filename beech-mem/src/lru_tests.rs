#![allow(unused_imports)]
use super::*;
use crate::mmap::MappedBuffer;
use std::sync::Arc;

fn buf(len: usize) -> Arc<MappedBuffer> {
    MappedBuffer::anonymous(len, None).unwrap()
}

#[test]
fn evicts_oldest() {
    // Capacity in bytes is 200 — three 100-byte buffers force one eviction.
    let mut cache: Cache<i32> = Cache::new(200);
    cache.put(1, buf(100));
    cache.put(2, buf(100));
    assert!(cache.contains(&1));
    assert!(cache.contains(&2));
    cache.put(3, buf(100));
    assert!(!cache.contains(&1), "key 1 should have been evicted");
    assert!(cache.contains(&2));
    assert!(cache.contains(&3));
}
