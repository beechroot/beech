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

use beech_core::wire::encode_node;
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
    /// Strategy: navigate to the affected leaf, collect rows from the
    /// affected leaf and every leaf to its right (i.e., all leaves whose
    /// keys are >= the affected leaf's lowest key), apply the change to
    /// that row list, re-shape into new leaves, and cascade upward
    /// rebuilding each ancestor from its left-unchanged children plus
    /// the new children from below.
    ///
    /// Correctness relies on a determinism property of `ProbShaper`:
    /// feeding identical byte sequences through a fresh shaper always
    /// produces the same split points. Left-unchanged children at every
    /// level therefore carry the same Ids as in the original tree, and
    /// the re-shape from the affected point rightward produces exactly
    /// the same nodes a fresh batch build of the final row-set would.
    pub fn apply(&mut self, change: super::Change) -> Result<()> {
        if self.virtual_root.is_none() {
            return self.apply_to_empty(change);
        }
        self.apply_incremental(change)
    }

    fn apply_to_empty(&mut self, change: super::Change) -> Result<()> {
        match change {
            super::Change::Insert {
                key,
                row_id,
                record,
            } => {
                let row = RowRecord {
                    key,
                    row_id,
                    values: record,
                };
                let leaves = self.build_leaves(vec![row])?;
                let children: Vec<ChildSummary> = leaves
                    .into_iter()
                    .map(|l| ChildSummary {
                        id: l.id,
                        highest_key: l.highest_key,
                        row_count: l.row_count,
                        depth: 0,
                    })
                    .collect();
                self.virtual_root = Some(self.finalize_root(children)?);
                Ok(())
            }
            super::Change::Update { key, .. } | super::Change::Delete { key } => {
                Err(DomainError::KeyNotFound { key }.into())
            }
        }
    }

    fn apply_incremental(&mut self, change: super::Change) -> Result<()> {
        // Walk from virtual_root to the affected leaf, remembering each
        // ancestor and the slot we descended through. At the leaf, we'll
        // apply the change to the rows in the leaf (plus all rows to the
        // right of it, collected by traversing right-side subtrees of
        // every ancestor).
        let path = self.locate(change.key())?;

        // Collect rows: affected leaf's rows, then each ancestor's right-
        // side subtrees in deepest-ancestor-first order (which gives rows
        // in ascending key order).
        let mut rows: Vec<RowRecord> = Vec::new();
        match &*path.leaf_node {
            Node::Leaf(leaf) => {
                for entry in &leaf.entries {
                    rows.push(RowRecord {
                        key: entry.key(&self.table),
                        row_id: entry.row.0,
                        values: entry.row.1.clone(),
                    });
                }
            }
            Node::Internal(_) => {
                return Err(beech_core::QueryError::UnexpectedNodeType {
                    expected: "leaf",
                    got: "branch",
                }
                .into());
            }
        }
        for step in path.ancestors.iter().rev() {
            if let Node::Internal(internal) = &*step.node {
                for right_child_id in &internal.children[step.slot + 1..] {
                    self.collect_rows_from(right_child_id, &mut rows)?;
                }
            }
        }

        // Apply the change to the row slice.
        apply_change_sorted(&mut rows, change)?;

        // Re-shape rows into new leaves. Empty result means this whole
        // right-of-and-including-affected subtree is now empty.
        let mut current_level: Vec<ChildSummary> = if rows.is_empty() {
            Vec::new()
        } else {
            self.build_leaves(rows)?
                .into_iter()
                .map(|l| ChildSummary {
                    id: l.id,
                    highest_key: l.highest_key,
                    row_count: l.row_count,
                    depth: 0,
                })
                .collect()
        };

        // Cascade upward. At each ancestor, left-unchanged children keep
        // their original Ids; the right side (including affected) is
        // replaced by `current_level`. Re-shape the combined list to
        // produce the next level's child summaries.
        for step in path.ancestors.iter().rev() {
            let Node::Internal(internal) = &*step.node else {
                return Err(beech_core::QueryError::UnexpectedNodeType {
                    expected: "branch",
                    got: "leaf",
                }
                .into());
            };
            // Left-unchanged children (slot 0..step.slot). Build their
            // ChildSummary: id from internal.children[i], highest_key from
            // internal.keys[i] (parent holds the highest_key of child i),
            // row_count via subtree_row_count for internal nodes or leaf
            // load for leaf nodes, depth from the subtree height.
            let left_depth = internal.subtree_height as i32 - 1;
            let mut combined = Vec::with_capacity(step.slot + current_level.len());
            for i in 0..step.slot {
                let child_id = internal.children[i].clone();
                let highest_key = internal.keys[i].clone();
                let row_count = self.row_count_of(&child_id)?;
                combined.push(ChildSummary {
                    id: child_id,
                    highest_key,
                    row_count,
                    depth: left_depth,
                });
            }
            combined.extend(current_level.drain(..));

            current_level = if combined.is_empty() {
                Vec::new()
            } else {
                self.build_one_branch_level(combined)?
            };
        }

        // Determine the new virtual_root. If empty, tree is empty. If one
        // element, it's the new root. If multiple, keep building branch
        // levels until one remains.
        self.virtual_root = match current_level.len() {
            0 => None,
            1 => Some(current_level.into_iter().next().expect("len 1").id),
            _ => Some(self.finalize_root(current_level)?),
        };
        Ok(())
    }

    /// Navigate from `virtual_root` to the leaf whose key range contains
    /// `target`, recording every ancestor and the slot we descended
    /// through. Panics if `virtual_root` is None (caller must handle).
    fn locate(&self, target: &Key) -> Result<Path> {
        let root_id = self
            .virtual_root
            .clone()
            .expect("locate called on empty tree");
        let mut ancestors = Vec::new();
        let mut current_id = root_id;
        loop {
            let node = self.get_node(&current_id, &self.table.schema)?;
            match &*node {
                Node::Internal(internal) => {
                    let slot = slot_for_key(&internal.keys, target);
                    let child_id = internal.children[slot].clone();
                    ancestors.push(PathStep {
                        node: Arc::clone(&node),
                        slot,
                    });
                    current_id = child_id;
                }
                Node::Leaf(_) => {
                    return Ok(Path {
                        ancestors,
                        leaf_id: current_id,
                        leaf_node: node,
                    });
                }
            }
        }
    }

    /// Return the number of rows in the subtree rooted at `id`. For
    /// internal nodes, uses subtree_row_count metadata. For leaves,
    /// loads the leaf to count entries.
    fn row_count_of(&self, id: &Id) -> Result<i64> {
        let node = self.get_node(id, &self.table.schema)?;
        Ok(node.row_count())
    }

    /// Build one branch level above the given children: feed each
    /// child's highest_key through a fresh ProbShaper and split when
    /// it fires, emitting new ChildSummary (branches) for the next
    /// level up.
    fn build_one_branch_level(&mut self, children: Vec<ChildSummary>) -> Result<Vec<ChildSummary>> {
        let mut result: Vec<ChildSummary> = Vec::new();
        let mut shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
        let mut batch: Vec<ChildSummary> = Vec::new();
        for child in children {
            let bytes = super::serialize_key_for_splitting(&child.highest_key)?;
            batch.push(child);
            if shaper.is_complete(&bytes) {
                let parent = self.finish_branch(std::mem::take(&mut batch), &shaper);
                result.push(parent);
                shaper = ProbShaper::new(self.target_node_size, self.node_size_stddev);
            }
        }
        if !batch.is_empty() {
            let parent = self.finish_branch(batch, &shaper);
            result.push(parent);
        }
        Ok(result)
    }

    /// Given a list of children at any depth, keep building branch
    /// levels until exactly one remains; return its Id.
    fn finalize_root(&mut self, mut children: Vec<ChildSummary>) -> Result<Id> {
        while children.len() > 1 {
            children = self.build_one_branch_level(children)?;
        }
        Ok(children.into_iter().next().expect("non-empty children").id)
    }

    /// Recursively collect every leaf row in the subtree rooted at `node_id`,
    /// traversing through the overlay where present.
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

    /// Serialize every overlay node through the writer and return the
    /// final virtual_root id. Does NOT write the Table/Transaction/root
    /// files; the caller finalizes those using the returned id (or
    /// `None` for an empty tree).
    ///
    /// Each node is encoded under its content-addressed Id via the same
    /// wire format used by `write_rows_to_prolly_tree`, so the on-disk
    /// result is byte-equal to a fresh batch build of the final row set.
    pub fn commit<W: super::Writer>(self, writer: &mut W) -> Result<Option<Id>> {
        let Self {
            overlay,
            virtual_root,
            table,
            ..
        } = self;
        for (id, node) in overlay {
            let bytes = encode_node(&node, id.clone(), &table.schema)?;
            writer.write(format!("{id}.bch"), &bytes)?;
        }
        Ok(virtual_root)
    }

    /// Discard the overlay without writing anything.
    pub fn rollback(self) {
        // Drops `self.overlay`; the underlying source is untouched.
    }
}

// --- Local helpers / types -------------------------------------------

/// A traversal path from virtual_root to the leaf containing a given key.
#[derive(Debug)]
struct Path {
    /// Internal nodes visited along the way, in root-to-leaf order. Each
    /// `slot` is the child index we descended through at that node.
    ancestors: Vec<PathStep>,
    #[allow(dead_code)]
    leaf_id: Id,
    leaf_node: Arc<Node>,
}

#[derive(Debug)]
struct PathStep {
    node: Arc<Node>,
    slot: usize,
}

/// Binary search over an InternalNode's `keys` (which are the highest_key
/// of each child except the last) to find the child slot whose subtree
/// contains `target`. Returns an index in `[0, children.len())`.
fn slot_for_key(keys: &[Key], target: &Key) -> usize {
    // keys[i] is children[i].highest_key for i in 0..keys.len().
    // children.len() == keys.len() + 1. Target belongs in the first
    // child whose highest_key >= target, defaulting to the last child
    // when target exceeds every key.
    keys.binary_search_by(|k| k.compare_key(target))
        .unwrap_or_else(|i| i)
}

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
