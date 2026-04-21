//! Transaction buffer and node cache for read-write tree operations.
//!
//! - `NodeCache`: decoded-node cache wrapping an existing `NodeSource`.
//!   Lives for the connection. Content-addressed nodes are immutable, so
//!   cached entries never go stale.
//!
//! - `TransactionBuffer`: incremental copy-on-write overlay for a single
//!   write transaction. Accumulates modified nodes under their new
//!   content-addressed Ids. Reads check the overlay first, then fall
//!   through to the underlying source. On commit, every overlay node is
//!   serialized to the caller's `Writer`. The final tree is byte-equal
//!   to what a fresh batch rebuild of the same final row-set would
//!   produce (prolly-canonical invariant).

use beech_core::{
    DomainError, Id, InternalNode, Key, KeyOrdering, LeafEntry, LeafNode, Node, NodeSource, Result,
    Root, Table, TableSchema, Transaction,
};
use beech_shaper::ProbShaper;
use std::collections::HashMap;
use std::sync::Arc;

// =============================================================================
// NodeCache
// =============================================================================

/// Decoded-node cache wrapping an inner `NodeSource`.
///
/// Because nodes are content-addressed (the Id is a hash of the content),
/// a cached node is guaranteed correct for all time — there is no
/// invalidation needed.
pub struct NodeCache<S: NodeSource> {
    #[allow(dead_code)]
    inner: S,
    // cache: HashMap<Id, Arc<Node>> or similar
}

impl<S: NodeSource> NodeCache<S> {
    /// Wrap an existing `NodeSource` with a decoded-node cache.
    pub fn new(_inner: S) -> Self {
        unimplemented!()
    }

    /// Number of decoded nodes currently cached.
    pub fn cached_node_count(&self) -> usize {
        unimplemented!()
    }

    /// Drop all cached nodes, reclaiming memory.
    pub fn clear(&mut self) {
        unimplemented!()
    }
}

impl<S: NodeSource> NodeSource for NodeCache<S> {
    fn get_root(&self) -> Result<Arc<Root>> {
        unimplemented!()
    }

    fn get_transaction(&self, _transaction_id: &Id) -> Result<Arc<Transaction>> {
        unimplemented!()
    }

    fn get_table(&self, _transaction: &Transaction, _table_name: &str) -> Result<Arc<Table>> {
        unimplemented!()
    }

    fn get_node(&self, _node_id: &Id, _schema: &TableSchema) -> Result<Arc<Node>> {
        unimplemented!()
    }
}

// =============================================================================
// TransactionBuffer
// =============================================================================

/// Incremental copy-on-write overlay for a single write transaction.
///
/// Holds the current virtual root Id and a map of modified/new nodes by
/// their computed Ids. Reads implemented via the `NodeSource` trait check
/// the overlay first, then fall through to the underlying source.
pub struct TransactionBuffer<S: NodeSource> {
    source: S,
    /// The table we're mutating. Its `root` field is the snapshot starting
    /// point; as changes are applied, `virtual_root` diverges.
    table: Arc<Table>,
    /// Root of the virtual tree reflecting applied changes. Starts at
    /// `table.root`; `None` when the tree is empty.
    virtual_root: Option<Id>,
    /// Modified or newly-constructed nodes, keyed by their content-
    /// addressed Id. Nodes not present here fall through to `source`.
    overlay: HashMap<Id, Arc<Node>>,
    target_node_size: usize,
    node_size_stddev: usize,
}

impl<S: NodeSource> TransactionBuffer<S> {
    /// Start a new write transaction against the given source and table.
    ///
    /// `target_node_size` and `node_size_stddev` must match the values
    /// used when the table's existing tree was built, otherwise the
    /// prolly-canonical invariant is broken.
    pub fn new(
        source: S,
        table: Arc<Table>,
        target_node_size: usize,
        node_size_stddev: usize,
    ) -> Self {
        let virtual_root = table.root.clone();
        Self {
            source,
            table,
            virtual_root,
            overlay: HashMap::new(),
            target_node_size,
            node_size_stddev,
        }
    }

    /// Apply one change, mutating the virtual tree in place.
    ///
    /// Phase B implementation: collect all rows from the current virtual
    /// tree, apply the change via a sorted merge, and rebuild the full
    /// tree in memory. Correct but O(n) per change. Phase C will replace
    /// this with local re-shape + cascade.
    pub fn apply(&mut self, change: super::Change) -> Result<()> {
        let mut rows = self.collect_all_rows()?;
        apply_change_sorted(&mut rows, change)?;
        self.rebuild_from_rows(rows)
    }

    /// Walk the current virtual tree (via self as a NodeSource, so the
    /// overlay shows through) and collect every leaf row.
    fn collect_all_rows(&self) -> Result<Vec<RowRecord>> {
        let Some(root_id) = self.virtual_root.clone() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        self.collect_rows_from(&root_id, &mut out)?;
        Ok(out)
    }

    fn collect_rows_from(&self, node_id: &Id, out: &mut Vec<RowRecord>) -> Result<()> {
        let node = self.get_node(node_id, &self.table.schema)?;
        match &*node {
            Node::Leaf(leaf) => {
                for entry in &leaf.entries {
                    out.push(RowRecord {
                        key: entry.key(&self.table),
                        row_id: entry.row.0,
                        values: entry.row.1.clone(),
                    });
                }
            }
            Node::Internal(internal) => {
                for child_id in &internal.children {
                    self.collect_rows_from(child_id, out)?;
                }
            }
        }
        Ok(())
    }

    /// Rebuild the tree in memory from a sorted row list, installing all
    /// new nodes in the overlay and updating `virtual_root`.
    fn rebuild_from_rows(&mut self, rows: Vec<RowRecord>) -> Result<()> {
        if rows.is_empty() {
            self.virtual_root = None;
            return Ok(());
        }

        let leaves = self.build_leaves(rows)?;
        let root_id = self.build_tree_from_leaves(leaves)?;
        self.virtual_root = Some(root_id);
        Ok(())
    }

    /// Feed rows through a fresh ProbShaper, emitting a LeafSummary at each
    /// split. Every new leaf is installed in the overlay under its
    /// computed Id.
    fn build_leaves(&mut self, rows: Vec<RowRecord>) -> Result<Vec<LeafSummary>> {
        let mut summaries = Vec::new();
        let mut shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
        let mut batch: Vec<RowRecord> = Vec::new();

        for row in rows {
            let bytes = super::serialize_row_for_splitting(&row.values)?;
            batch.push(row);
            if shaper.is_complete(&bytes) {
                let summary = self.finish_leaf(std::mem::take(&mut batch), &shaper);
                summaries.push(summary);
                shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
            }
        }

        if !batch.is_empty() {
            let summary = self.finish_leaf(batch, &shaper);
            summaries.push(summary);
        }

        Ok(summaries)
    }

    fn finish_leaf(&mut self, rows: Vec<RowRecord>, shaper: &ProbShaper) -> LeafSummary {
        let id: Id = shaper.id().into();
        let keys: Vec<Key> = rows.iter().map(|r| r.key.clone()).collect();
        let highest_key = keys.last().cloned().expect("non-empty batch");
        let row_count = rows.len() as i64;
        let entries: Vec<LeafEntry> = rows
            .into_iter()
            .map(|r| LeafEntry {
                row: (r.row_id, r.values),
            })
            .collect();
        let node = Node::Leaf(LeafNode { keys, entries });
        self.overlay.insert(id.clone(), Arc::new(node));
        LeafSummary {
            id,
            highest_key,
            row_count,
        }
    }

    /// Build internal nodes level by level until a single root remains.
    fn build_tree_from_leaves(&mut self, leaves: Vec<LeafSummary>) -> Result<Id> {
        let mut current: Vec<ChildSummary> = leaves
            .into_iter()
            .map(|l| ChildSummary {
                id: l.id,
                highest_key: l.highest_key,
                row_count: l.row_count,
                depth: 0,
            })
            .collect();

        while current.len() > 1 {
            let mut next: Vec<ChildSummary> = Vec::new();
            let mut shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
            let mut batch: Vec<ChildSummary> = Vec::new();

            for child in current {
                let bytes = super::serialize_key_for_splitting(&child.highest_key)?;
                batch.push(child);
                if shaper.is_complete(&bytes) {
                    let parent = self.finish_branch(std::mem::take(&mut batch), &shaper);
                    next.push(parent);
                    shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
                }
            }
            if !batch.is_empty() {
                let parent = self.finish_branch(batch, &shaper);
                next.push(parent);
            }
            current = next;
        }

        Ok(current.into_iter().next().expect("non-empty tree").id)
    }

    fn finish_branch(&mut self, children: Vec<ChildSummary>, shaper: &ProbShaper) -> ChildSummary {
        let id: Id = shaper.id().into();
        let depth = children[0].depth + 1;
        let row_count: i64 = children.iter().map(|c| c.row_count).sum();
        let highest_key = children
            .last()
            .expect("non-empty batch")
            .highest_key
            .clone();
        let child_count = children.len();
        let child_ids: Vec<Id> = children.iter().map(|c| c.id.clone()).collect();
        let keys: Vec<Key> = if child_count > 1 {
            children[..child_count - 1]
                .iter()
                .map(|c| c.highest_key.clone())
                .collect()
        } else {
            Vec::new()
        };
        let node = Node::Internal(InternalNode {
            keys,
            children: child_ids,
            subtree_height: depth as u32,
            subtree_row_count: row_count as u64,
        });
        self.overlay.insert(id.clone(), Arc::new(node));
        ChildSummary {
            id,
            highest_key,
            row_count,
            depth,
        }
    }

    /// Current virtual root id (None if the tree is empty).
    pub fn virtual_root(&self) -> Option<&Id> {
        self.virtual_root.as_ref()
    }

    /// Number of overlay nodes currently held.
    pub fn overlay_size(&self) -> usize {
        self.overlay.len()
    }

    /// Reference to the table this buffer was opened against.
    pub fn table(&self) -> &Table {
        &self.table
    }

    /// Serialize every overlay node to the writer. Does NOT write the
    /// Table/Transaction/root files; that's the caller's job using the
    /// returned virtual_root id.
    pub fn commit<W: super::Writer>(self, _writer: &mut W) -> Result<Option<Id>> {
        unimplemented!("Phase D")
    }

    /// Discard the overlay without writing anything.
    pub fn rollback(self) {
        // Drops `self.overlay`; the underlying source is untouched.
    }
}

// --- Local helpers / types -------------------------------------------

#[derive(Debug, Clone)]
struct RowRecord {
    key: Key,
    row_id: i64,
    values: Vec<apache_avro::types::Value>,
}

#[derive(Debug, Clone)]
struct LeafSummary {
    id: Id,
    highest_key: Key,
    row_count: i64,
}

#[derive(Debug, Clone)]
struct ChildSummary {
    id: Id,
    highest_key: Key,
    row_count: i64,
    depth: i32,
}

/// Apply a single change to a sorted row list, preserving sort order.
/// Mirrors the insert/update/delete semantics of `merge_changes`.
fn apply_change_sorted(rows: &mut Vec<RowRecord>, change: super::Change) -> Result<()> {
    let key = change.key().clone();
    let search = rows.binary_search_by(|r| r.key.compare_key(&key));
    match change {
        super::Change::Insert {
            key,
            row_id,
            record,
        } => match search {
            Ok(_) => Err(DomainError::DuplicateKey { key }.into()),
            Err(pos) => {
                rows.insert(
                    pos,
                    RowRecord {
                        key,
                        row_id,
                        values: record,
                    },
                );
                Ok(())
            }
        },
        super::Change::Update {
            key,
            row_id,
            record,
        } => match search {
            Ok(pos) => {
                rows[pos] = RowRecord {
                    key,
                    row_id,
                    values: record,
                };
                Ok(())
            }
            Err(_) => Err(DomainError::KeyNotFound { key }.into()),
        },
        super::Change::Delete { key } => match search {
            Ok(pos) => {
                rows.remove(pos);
                Ok(())
            }
            Err(_) => Err(DomainError::KeyNotFound { key }.into()),
        },
    }
}

impl<S: NodeSource> NodeSource for TransactionBuffer<S> {
    fn get_root(&self) -> Result<Arc<Root>> {
        self.source.get_root()
    }

    fn get_transaction(&self, transaction_id: &Id) -> Result<Arc<Transaction>> {
        self.source.get_transaction(transaction_id)
    }

    fn get_table(&self, transaction: &Transaction, table_name: &str) -> Result<Arc<Table>> {
        self.source.get_table(transaction, table_name)
    }

    /// Check the overlay first; fall through to the underlying source on
    /// miss. Since every Id is content-addressed, a hit in the overlay
    /// is always the canonical, correct node for that Id.
    fn get_node(&self, node_id: &Id, schema: &TableSchema) -> Result<Arc<Node>> {
        if let Some(node) = self.overlay.get(node_id) {
            return Ok(Arc::clone(node));
        }
        self.source.get_node(node_id, schema)
    }
}
