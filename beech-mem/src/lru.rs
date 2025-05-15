use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use crate::mmap::MappedBuffer;

use indexmap::IndexMap;

pub struct Cache<K> {
    map: HashMap<K, Arc<MappedBuffer>>,
    evict_order: IndexMap<K, ()>,
    current_size: usize,
    max_size: usize,
}

impl<K> Cache<K>
where
    K: Hash + Eq + Clone,
{
    pub fn new(max_size: usize) -> Self {
        Self {
            map: HashMap::new(),
            evict_order: IndexMap::new(),
            current_size: 0,
            max_size,
        }
    }

    pub fn get(&mut self, key: &K) -> Option<Arc<MappedBuffer>> {
        if let Some(value) = self.map.get(key) {
            // Promote to most recently used
            self.evict_order.shift_remove(key);
            self.evict_order.insert(key.clone(), ());
            Some(value.clone())
        } else {
            None
        }
    }

    pub fn peek(&self, key: &K) -> Option<&Arc<MappedBuffer>> {
        self.map.get(key)
    }

    pub fn contains(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn put(&mut self, key: K, value: Arc<MappedBuffer>) {
        if let Some(old) = self.map.insert(key.clone(), value.clone()) {
            self.current_size -= old.len();
            self.evict_order.shift_remove(&key);
        }

        self.current_size += value.len();
        self.evict_order.insert(key.clone(), ());

        while self.current_size > self.max_size {
            if let Some((oldest_key, _)) = self.evict_order.shift_remove_index(0) {
                if let Some(old_value) = self.map.remove(&oldest_key) {
                    self.current_size -= old_value.len();
                }
            }
        }
    }

    pub fn current_size(&self) -> usize {
        self.current_size
    }
}
