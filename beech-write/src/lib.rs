use apache_avro::{
    Schema,
    schema::{Name, RecordField},
    types::Value,
};
use beech_core::{
    BeechError, Id, Key, KeyOrdering, NodeSource, Page, Result, Root, Row, Table, Transaction,
    query::{Constraint, ConstraintOp, Cursor},
    wire::{encode_page, encode_root, encode_table, encode_transaction, find_key_columns},
};
use beech_shaper::ProbShaper;
use itertools::Itertools;
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap},
    path::Path,
    time::SystemTime,
};

/// Writer trait that can write to either filesystem or memory
pub trait Writer {
    fn write<P>(&mut self, name: P, data: &[u8]) -> std::io::Result<()>
    where
        P: AsRef<Path>;
    fn commit(self) -> std::io::Result<()>;
    fn abort(self) -> std::io::Result<()>;
    fn num_to_commit(&self) -> usize;
}

#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub id: Id,
    pub highest_key: Key,
    pub depth: i32,
    pub row_count: i64,
}

/// Represents a change to apply to a table
#[derive(Debug, Clone)]
pub enum Change {
    /// Insert a new row with the given key and record data
    Insert {
        key: Key,
        row_id: i64,
        record: Vec<Value>,
    },
    /// Update an existing row with the given key
    Update {
        key: Key,
        row_id: i64,
        record: Vec<Value>,
    },
    /// Delete a row with the given key
    Delete { key: Key },
}

impl Change {
    /// Get the key for this change
    pub fn key(&self) -> &Key {
        match self {
            Change::Insert { key, .. } | Change::Update { key, .. } | Change::Delete { key } => key,
        }
    }
}

/// Merge changes into an existing table tree
///
/// This function processes changes in a bottom-up manner without recursion:
/// 1. Process changes at the leaf level
/// 2. Update branch pages based on leaf changes
/// 3. Continue up to the root
///
/// # Arguments
/// * `changes` - Iterator of changes pre-sorted by primary key
/// * `root_id` - Root page ID of the existing table (None for empty table)
/// * `table` - The table metadata
/// * `node_source` - Source for reading existing pages
/// * `writer` - Transactional writer for new/modified pages
/// * `target_page_size` - Target size for pages
/// * `page_size_stddev` - Standard deviation for probabilistic splitting
///
/// # Returns
/// The new root page ID after applying all changes, or None if no root exists
pub fn merge_changes<I, NS, W>(
    mut changes: std::iter::Peekable<I>,
    table: &Table,
    node_source: &NS,
    writer: &mut W,
    target_page_size: usize,
    page_size_stddev: usize,
) -> Result<Option<Id>>
where
    I: Iterator<Item = Change>,
    NS: NodeSource,
    W: Writer,
{
    // Handle empty changes - return current root trivially (never fails)
    let Some(first_change) = changes.peek() else {
        return Ok(table.root.clone());
    };

    // Phase 1: Process leaf pages
    let mut leaf_metadata = Vec::new();

    if table.root.is_none() {
        let updated_pages = apply_changes_to_leaf(
            &[],
            &[],
            changes,
            &table.key_scheme,
            &table.row_scheme,
            target_page_size,
            page_size_stddev,
        )?;

        // Write pages and collect metadata
        for page_meta in updated_pages {
            let page_id =
                write_page_metadata(writer, &page_meta, &table.key_scheme, &table.row_scheme)?;
            leaf_metadata.push(PageMetadata {
                id: page_id,
                highest_key: page_meta.highest_key,
                depth: 0,
                row_count: page_meta.row_count,
            });
        }

        // Build tree from leaf metadata and return root
        return build_tree_from_leaves(
            leaf_metadata,
            target_page_size,
            page_size_stddev,
            writer,
            &table.key_scheme,
            &table.row_scheme,
        );
    }

    // Create cursor to navigate to first leaf, starting from first change key if available
    let mut cursor = Cursor::new(table);

    // Create constraint to start at the first change's key
    let (constraints, values) = create_constraint_from_key(first_change.key(), table)?;
    cursor.init(constraints, values);
    cursor.advance_to_left(node_source)?;

    while !cursor.eof() && changes.peek().is_some() {
        // Get current leaf page
        let (leaf_id, _) = cursor
            .current()
            .ok_or_else(|| BeechError::InternalError("Cursor has no current position".to_string()))?;
        let leaf_page = node_source.get_page(leaf_id, &table.key_scheme, &table.row_scheme)?;

        // Determine the highest key in this leaf to know when to stop collecting changes
        let Page::Leaf { keys, rows } = &*leaf_page else {
            return Err(BeechError::Corrupt("Expected leaf page but found branch page".to_string()));
        };
        let Some(leaf_highest_key) = keys.last() else {
            return Err(BeechError::Corrupt("Empty leaf page encountered".to_string()));
        };

        // Create an iterator that takes changes for this leaf
        let leaf_changes_iter = std::iter::from_fn(|| {
            if let Some(change) = changes.peek() {
                let change_key = change.key();
                // Use KeyOrdering trait for proper key comparison
                if change_key.compare_key(leaf_highest_key) == Ordering::Greater {
                    return None;
                }

                changes.next()
            } else {
                None
            }
        });

        let updated_pages = apply_changes_to_leaf(
            keys,
            rows,
            leaf_changes_iter,
            &table.key_scheme,
            &table.row_scheme,
            target_page_size,
            page_size_stddev,
        )?;

        // Write updated pages and collect metadata
        for page_meta in updated_pages {
            // Write page to storage
            let page_id =
                write_page_metadata(writer, &page_meta, &table.key_scheme, &table.row_scheme)?;
            leaf_metadata.push(PageMetadata {
                id: page_id,
                highest_key: page_meta.highest_key,
                depth: 0, // Leaf pages have depth 0
                row_count: page_meta.row_count,
            });
        }

        // Move to next leaf
        cursor.advance_to_next_leaf(node_source)?;
    }

    // Handle any remaining changes that go beyond existing leaves
    if changes.peek().is_some() {
        let remaining_pages = apply_changes_to_leaf(
            &[], // No existing keys for new leaves
            &[], // No existing rows for new leaves
            changes,
            &table.key_scheme,
            &table.row_scheme,
            target_page_size,
            page_size_stddev,
        )?;

        // Write pages and collect metadata
        for page_meta in remaining_pages {
            let page_id =
                write_page_metadata(writer, &page_meta, &table.key_scheme, &table.row_scheme)?;
            leaf_metadata.push(PageMetadata {
                id: page_id,
                highest_key: page_meta.highest_key,
                depth: 0,
                row_count: page_meta.row_count,
            });
        }
    }

    // Build tree from all leaf metadata and return root
    build_tree_from_leaves(
        leaf_metadata,
        target_page_size,
        page_size_stddev,
        writer,
        &table.key_scheme,
        &table.row_scheme,
    )
}

// Helper functions that would need to be implemented
fn apply_changes_to_leaf<I>(
    existing_keys: &[Key],
    existing_rows: &[Row],
    changes: I,
    _key_scheme: &Schema,
    _row_scheme: &Schema,
    target_page_size: usize,
    page_size_stddev: usize,
) -> Result<Vec<PageMetadata>>
where
    I: Iterator<Item = Change>,
{
    // Step 1: Load all existing records into a mutable collection
    // Use Vec since Key (Vec<Value>) doesn't implement Hash/Eq for HashMap
    let mut records: Vec<(Key, (i64, Vec<Value>))> = Vec::new();

    // Load existing records
    for (key, (row_id, row_values)) in existing_keys.iter().zip(existing_rows.iter()) {
        records.push((key.clone(), (*row_id, row_values.clone())));
    }

    // Step 2: Apply all changes to the records
    for change in changes {
        match change {
            Change::Insert {
                key,
                row_id,
                record,
            } => {
                // Check for duplicate key
                if records
                    .iter()
                    .any(|(k, _)| k.compare_key(&key) == std::cmp::Ordering::Equal)
                {
                    return Err(BeechError::DuplicateKey(format!("{:?}", key)));
                }
                records.push((key, (row_id, record)));
            }
            Change::Update {
                key,
                row_id,
                record,
            } => {
                // Update existing record
                if let Some(pos) = records
                    .iter()
                    .position(|(k, _)| k.compare_key(&key) == std::cmp::Ordering::Equal)
                {
                    records[pos] = (key, (row_id, record));
                } else {
                    return Err(BeechError::NotFound(format!("{:?}", key)));
                }
            }
            Change::Delete { key } => {
                // Delete existing record
                if let Some(pos) = records
                    .iter()
                    .position(|(k, _)| k.compare_key(&key) == std::cmp::Ordering::Equal)
                {
                    records.remove(pos);
                } else {
                    return Err(BeechError::NotFound(format!("{:?}", key)));
                }
            }
        }
    }

    // Step 3: Convert back to sorted records and split if necessary
    let mut sorted_records: Vec<(Key, (i64, Vec<Value>))> = records.into_iter().collect();
    // Sort by key using KeyOrdering trait
    sorted_records.sort_by(|(k1, _), (k2, _)| k1.compare_key(k2));

    if sorted_records.is_empty() {
        // This leaf is now empty - return empty metadata
        return Ok(vec![]);
    }

    // Step 4: Split records into pages using probabilistic splitting
    let mut result_pages = Vec::new();
    let mut current_records = Vec::new();
    let mut shaper = ProbShaper::new(target_page_size, page_size_stddev);

    for (key, (row_id, row_values)) in sorted_records {
        current_records.push((key.clone(), (row_id, row_values)));

        // Check if we should split
        let key_bytes = serialize_key_for_splitting(&key)?;
        if shaper.is_complete(&key_bytes) && !current_records.is_empty() {
            // Create a page from current records
            let page_metadata = create_page_metadata_from_records(current_records, &mut shaper)?;
            result_pages.push(page_metadata);
            current_records = Vec::new();
            shaper = ProbShaper::new(target_page_size, page_size_stddev);
        }
    }

    // Don't forget the last page
    if !current_records.is_empty() {
        let page_metadata = create_page_metadata_from_records(current_records, &mut shaper)?;
        result_pages.push(page_metadata);
    }

    Ok(result_pages)
}

fn write_page_metadata<W: Writer>(
    _writer: &mut W,
    metadata: &PageMetadata,
    _key_scheme: &Schema,
    _row_scheme: &Schema,
) -> Result<Id> {
    // This function should write a leaf page from metadata
    // The metadata doesn't contain the actual keys/rows, so this seems problematic
    // For now, return the ID that was already generated
    // TODO: This function needs redesign - metadata doesn't contain actual page data
    Ok(metadata.id.clone())
}

fn create_branch_from_children(children: &[PageMetadata]) -> Result<PageMetadata> {
    if children.is_empty() {
        return Err(BeechError::InternalError("Cannot create branch from empty children".to_string()));
    }

    // Get the highest key from the last child
    let highest_key = children.last().unwrap().highest_key.clone();

    // Calculate total row count
    let row_count: i64 = children.iter().map(|c| c.row_count).sum();

    // Depth is one more than the children's depth (assumes all children at same depth)
    let depth = children[0].depth + 1;

    // Generate a deterministic ID based on child IDs
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for child in children {
        hasher.update(child.id.as_bytes());
    }
    let hash = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash[..32]);
    let id = Id::from(id_bytes);

    Ok(PageMetadata {
        id,
        highest_key,
        depth,
        row_count,
    })
}

fn write_branch_page<W: Writer>(
    writer: &mut W,
    children: &[PageMetadata],
    key_scheme: &Schema,
    row_scheme: &Schema,
) -> Result<Id> {
    if children.is_empty() {
        return Err(BeechError::InternalError("Cannot write branch page with no children".to_string()));
    }

    // Extract the separator keys (highest key of each child except the last)
    let keys: Vec<Key> = children[..children.len() - 1]
        .iter()
        .map(|child| child.highest_key.clone())
        .collect();

    // Extract child page IDs
    let child_ids: Vec<Id> = children.iter().map(|child| child.id.clone()).collect();

    // Calculate branch metadata
    let depth = children[0].depth + 1;
    let row_count: i64 = children.iter().map(|c| c.row_count).sum();

    // Generate page ID based on children (deterministic)
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    for child in children {
        hasher.update(child.id.as_bytes());
    }
    let hash = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash[..32]);
    let page_id = Id::from(id_bytes);

    // Create and write the branch page
    let page = Page::Branch {
        keys,
        children: child_ids,
        depth,
        row_count,
    };

    let page_bytes = encode_page(&page, page_id.clone(), key_scheme, row_scheme)?;
    writer.write(file_name(&page_id), &page_bytes)?;

    Ok(page_id)
}

// Split functions for batching
fn split_leafs(
    _schema: &Schema,
    target_length: usize,
    stddev: usize,
    it: &mut impl Iterator<Item = (i64, Value, Vec<Value>)>,
) -> Option<(Id, Vec<(i64, Value, Vec<Value>)>)> {
    let mut shaper = ProbShaper::new(target_length, stddev);
    let mut current_batch = Vec::new();
    let min_rows_per_page = 10; // Minimum rows before considering a split

    for (row_id, key_value, row_values) in it {
        current_batch.push((row_id, key_value.clone(), row_values));

        // Only check for splitting after we have minimum rows
        if current_batch.len() >= min_rows_per_page {
            // Use the key value bytes to determine split point
            let key_bytes = match &key_value {
                Value::String(s) => s.as_bytes().to_vec(),
                Value::Long(l) => l.to_le_bytes().to_vec(),
                Value::Boolean(b) => vec![if *b { 1 } else { 0 }],
                Value::Float(f) => f.to_le_bytes().to_vec(),
                Value::Double(d) => d.to_le_bytes().to_vec(),
                Value::Bytes(b) => b.clone(),
                _ => format!("{key_value:?}").into_bytes(),
            };

            if shaper.is_complete(&key_bytes) {
                // Generate page ID using shaper
                let page_id: Id = shaper.clone().id().into();
                return Some((page_id, current_batch));
            }
        }
    }

    // Return the final batch if we have any items
    if !current_batch.is_empty() {
        let page_id: Id = shaper.clone().id().into();
        Some((page_id, current_batch))
    } else {
        None
    }
}

fn split_branch_metadata(
    _schema: &Schema,
    target_length: usize,
    stddev: usize,
    it: &mut impl Iterator<Item = PageMetadata>,
) -> Option<(Id, Vec<(Key, PageMetadata)>)> {
    let mut shaper = ProbShaper::new(target_length, stddev);
    let mut current_batch = Vec::new();

    for page_metadata in it {
        let key = page_metadata.highest_key.clone();
        current_batch.push((key.clone(), page_metadata));

        // Use the highest key bytes to determine split point
        let key_bytes =
            serialize_key_for_splitting(&key).unwrap_or_else(|_| format!("{key:?}").into_bytes());

        if shaper.is_complete(&key_bytes) && !current_batch.is_empty() {
            // Generate page ID using shaper
            let page_id: Id = shaper.clone().id().into();
            return Some((page_id, current_batch));
        }
    }

    // Return the final batch if we have any items
    if !current_batch.is_empty() {
        let page_id: Id = shaper.clone().id().into();
        Some((page_id, current_batch))
    } else {
        None
    }
}

/// Core function for writing rows to a prolly tree using any Writer implementation
pub fn write_rows_to_prolly_tree<W: Writer>(
    writer: &mut W,
    table_name: String,
    key_column_indices: Vec<usize>,
    rows: Vec<(i64, Value)>, // (row_id, record)
    target_page_size: usize,
    page_size_stddev: usize,
    prev_id: Option<Id>,
) -> Result<(Id, Id)> {
    // Extract schema from first record
    let row_schema = if let Some((_, first_record)) = rows.first() {
        infer_row_schema_from_record(first_record)?
    } else {
        return Err(BeechError::Args("No rows provided".to_string()));
    };

    let key_schema = extract_key_schema(&row_schema, &key_column_indices)?;
    let key_columns = find_key_columns(&key_schema, &row_schema)?;

    // Convert rows to the format expected by the tree builder
    let rows_with_data: Vec<(i64, Value, Vec<Value>)> = rows
        .into_iter()
        .map(|(row_id, record)| {
            let fields = record_to_fields(&record);
            (row_id, record, fields)
        })
        .collect();

    // Build prolly tree bottom-up
    let mut pages: Vec<PageMetadata> = vec![];

    // Create leaf pages
    for (page_id, page_rows) in rows_with_data
        .into_iter()
        .batching(|it| split_leafs(&row_schema, target_page_size, page_size_stddev, it))
    {
        let keys: Vec<Key> = page_rows
            .iter()
            .map(|(_, _, values)| key_from_row_values(&key_columns, values))
            .collect();

        let highest_key = keys
            .iter()
            .cloned()
            .reduce(|acc, key| acc.max_key(key))
            .ok_or_else(|| BeechError::InternalError("Empty page created".to_string()))?;

        // Convert to the format expected by Page::Leaf
        let leaf_rows: Vec<(i64, Vec<Value>)> = page_rows
            .into_iter()
            .map(|(row_id, _, values)| (row_id, values))
            .collect();

        let page = Page::Leaf {
            keys,
            rows: leaf_rows.clone(),
        };

        let page_bytes = encode_page(&page, page_id.clone(), &key_schema, &row_schema)?;
        writer.write(file_name(&page_id), &page_bytes)?;

        pages.push(PageMetadata {
            id: page_id,
            highest_key,
            depth: 0,                          // Leaf pages always have depth 0
            row_count: leaf_rows.len() as i64, // Count of rows in this leaf
        });
    }

    // Build branch pages
    while pages.len() > 1 {
        let mut new_pages: Vec<PageMetadata> = vec![];
        for (page_id, branch_entries) in pages.into_iter().batching(|it| {
            split_branch_metadata(&key_schema, target_page_size, page_size_stddev, it)
        }) {
            let (entry_keys, children_metadata): (Vec<_>, Vec<_>) =
                branch_entries.into_iter().unzip();
            let children: Vec<Id> = children_metadata.iter().map(|m| m.id.clone()).collect();

            // In prolly trees, branches have one fewer keys than children
            // Keys represent the separator values, not the max key in each child
            let keys = if entry_keys.len() > 1 {
                entry_keys[..entry_keys.len() - 1].to_vec()
            } else {
                vec![]
            };

            let highest_key = entry_keys
                .iter()
                .cloned()
                .reduce(|acc, key| acc.max_key(key))
                .ok_or_else(|| BeechError::InternalError("Empty branch page created".to_string()))?;

            // Calculate depth and row count from child metadata
            let mut total_row_count = 0i64;
            let mut max_child_depth = 0i32;

            for child_metadata in &children_metadata {
                total_row_count += child_metadata.row_count;
                max_child_depth = max_child_depth.max(child_metadata.depth);
            }

            let depth = max_child_depth + 1;
            let row_count = total_row_count;

            let page = Page::Branch {
                keys,
                children,
                depth,
                row_count,
            };
            let page_bytes = encode_page(&page, page_id.clone(), &key_schema, &row_schema)?;
            writer.write(file_name(&page_id), &page_bytes)?;

            new_pages.push(PageMetadata {
                id: page_id,
                highest_key,
                depth,
                row_count,
            });
        }
        pages = new_pages;
    }

    let root_page_metadata = pages.into_iter().next();
    let root_page_id = root_page_metadata.as_ref().map(|m| m.id.clone());

    // Create table
    let table_id = generate_table_id(root_page_id.as_ref(), &table_name);
    let table = Table::new(
        table_id.clone(),
        table_name.clone(),
        root_page_id,
        key_schema,
        row_schema,
    )?;
    let table_bytes = encode_table(
        &table,
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64,
    )?;
    writer.write(file_name(&table_id), &table_bytes)?;

    // Create transaction
    let transaction_time = SystemTime::now();
    let prev_id_for_transaction = prev_id.unwrap_or_default();
    let transaction_id = generate_transaction_id(&prev_id_for_transaction, &transaction_time);
    let mut tables = HashMap::new();
    tables.insert(table_name, table_id.clone());
    let transaction = Transaction {
        id: transaction_id.clone(),
        prev_id: prev_id_for_transaction,
        transaction_time,
        tables,
    };
    let transaction_bytes = encode_transaction(&transaction)?;
    writer.write(file_name(&transaction_id), &transaction_bytes)?;

    // Create root
    let root = Root {
        id: transaction_id.clone(),
    };
    let root_bytes = encode_root(&root)?;
    writer.write("root.bch", &root_bytes)?;

    Ok((transaction_id, table_id))
}

fn create_constraint_from_key(
    key: &Key,
    table: &Table,
) -> Result<(Vec<Constraint>, Vec<Value>)> {
    // Create equality constraints for each key column
    let mut constraints = Vec::new();
    let mut values = Vec::new();

    for (i, (col_idx, _col)) in table.key_columns.iter().enumerate() {
        // Add constraint for this key column
        constraints.push(Constraint::new(*col_idx as i32, ConstraintOp::Eq));

        // Add the corresponding value from the key
        if i < key.len() {
            values.push(key[i].clone());
        } else {
            return Err(BeechError::SchemaMismatch("Key has fewer values than key columns".to_string()));
        }
    }

    Ok((constraints, values))
}

fn file_name(id: &Id) -> String {
    format!("{id}.bch")
}

/// Generate a table ID based on the root page ID and table name
fn generate_table_id(root_page_id: Option<&Id>, table_name: &str) -> Id {
    let mut hasher = Sha256::new();

    if let Some(root_id) = root_page_id {
        hasher.update(root_id.as_bytes());
    }
    hasher.update(table_name.as_bytes());

    let hash_result = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash_result[..]);
    Id::from(id_bytes)
}

/// Generate a transaction ID based on the prev_id and transaction_time
fn generate_transaction_id(prev_id: &Id, transaction_time: &SystemTime) -> Id {
    let mut hasher = Sha256::new();
    hasher.update(prev_id.as_bytes());

    // Convert SystemTime to bytes for hashing
    let duration = transaction_time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    hasher.update(duration.as_secs().to_le_bytes());
    hasher.update(duration.subsec_nanos().to_le_bytes());

    let hash_result = hasher.finalize();
    let mut id_bytes = [0u8; 32];
    id_bytes.copy_from_slice(&hash_result[..]);
    Id::from(id_bytes)
}

pub fn infer_row_schema_from_record(record: &Value) -> Result<Schema> {
    match record {
        Value::Record(fields) => {
            let schema_fields: Result<Vec<_>> = fields
                .iter()
                .enumerate()
                .map(|(i, (name, value))| {
                    let field_schema = match value {
                        Value::String(_) => Schema::String,
                        Value::Long(_) => Schema::Long,
                        Value::Double(_) => Schema::Double,
                        Value::Int(_) => Schema::Int,
                        Value::Boolean(_) => Schema::Boolean,
                        _ => return Err(BeechError::UnsupportedType("Unsupported field type".to_string())),
                    };
                    Ok(RecordField {
                        name: name.clone(),
                        doc: None,
                        aliases: None,
                        default: None,
                        schema: field_schema,
                        order: apache_avro::schema::RecordFieldOrder::Ascending,
                        position: i,
                        custom_attributes: BTreeMap::new(),
                    })
                })
                .collect();

            let fields = schema_fields?;
            let mut lookup = BTreeMap::new();
            for (i, field) in fields.iter().enumerate() {
                lookup.insert(field.name.clone(), i);
            }

            Ok(Schema::Record(apache_avro::schema::RecordSchema {
                name: Name::new("Record")?,
                aliases: None,
                doc: None,
                fields,
                lookup,
                attributes: BTreeMap::new(),
            }))
        }
        _ => Err(BeechError::SchemaMismatch("Expected record value".to_string())),
    }
}

fn extract_key_schema(row_schema: &Schema, key_column_indices: &[usize]) -> Result<Schema> {
    if let Schema::Record(row_record) = row_schema {
        let key_fields: Result<Vec<_>> = key_column_indices
            .iter()
            .enumerate()
            .map(|(i, &col_idx)| {
                row_record
                    .fields
                    .get(col_idx)
                    .ok_or_else(|| BeechError::SchemaMismatch("Key column index out of bounds".to_string()))
                    .map(|field| RecordField {
                        name: field.name.clone(),
                        doc: None,
                        aliases: None,
                        default: None,
                        schema: field.schema.clone(),
                        order: apache_avro::schema::RecordFieldOrder::Ascending,
                        position: i,
                        custom_attributes: BTreeMap::new(),
                    })
            })
            .collect();

        let key_fields = key_fields?;
        let mut lookup = BTreeMap::new();
        for (i, field) in key_fields.iter().enumerate() {
            lookup.insert(field.name.clone(), i);
        }

        Ok(Schema::Record(apache_avro::schema::RecordSchema {
            name: Name::new("Key")?,
            aliases: None,
            doc: None,
            fields: key_fields,
            lookup,
            attributes: BTreeMap::new(),
        }))
    } else {
        Err(BeechError::SchemaMismatch("Expected record schema".to_string()))
    }
}

fn record_to_fields(record: &Value) -> Vec<Value> {
    match record {
        Value::Record(fields) => fields.iter().map(|(_, value)| value.clone()).collect(),
        _ => panic!("Expected record value"),
    }
}

fn key_from_row_values(key_columns: &[usize], row: &[Value]) -> Vec<Value> {
    key_columns.iter().map(|&i| row[i].clone()).collect()
}

/// Create PageMetadata from a collection of records
fn create_page_metadata_from_records(
    records: Vec<(Key, (i64, Vec<Value>))>,
    shaper: &mut ProbShaper,
) -> Result<PageMetadata> {
    if records.is_empty() {
        return Err(BeechError::InternalError("Cannot create page from empty records".to_string()));
    }

    // Extract keys and rows
    let keys: Vec<Key> = records.iter().map(|(key, _)| key.clone()).collect();
    let rows: Vec<(i64, Vec<Value>)> = records.into_iter().map(|(_, row)| row).collect();

    // Get highest key for this page
    let highest_key = keys.last().unwrap().clone();

    // Generate page ID using shaper (clone because id() consumes the shaper)
    let page_id: Id = shaper.clone().id().into();

    Ok(PageMetadata {
        id: page_id,
        highest_key,
        depth: 0, // Always 0 for leaf pages
        row_count: rows.len() as i64,
    })
}

/// Serialize key for probabilistic splitting decision
fn serialize_key_for_splitting(key: &Key) -> Result<Vec<u8>> {
    // Simple serialization - could be improved
    let mut bytes = Vec::new();
    for value in key {
        match value {
            Value::Long(n) => bytes.extend_from_slice(&n.to_le_bytes()),
            Value::String(s) => bytes.extend_from_slice(s.as_bytes()),
            Value::Int(n) => bytes.extend_from_slice(&n.to_le_bytes()),
            _ => bytes.extend_from_slice(format!("{value:?}").as_bytes()),
        }
        bytes.push(b'|'); // separator
    }
    Ok(bytes)
}

fn build_tree_from_leaves<W: Writer>(
    leaf_metadata: Vec<PageMetadata>,
    target_page_size: usize,
    page_size_stddev: usize,
    writer: &mut W,
    key_scheme: &Schema,
    row_scheme: &Schema,
) -> Result<Option<Id>> {
    if leaf_metadata.is_empty() {
        return Ok(None);
    }

    if leaf_metadata.len() == 1 {
        return Ok(Some(leaf_metadata[0].id.clone()));
    }

    // Phase 2: Build branch pages bottom-up
    let mut current_level = leaf_metadata;

    while current_level.len() > 1 {
        let mut next_level = Vec::new();
        let mut shaper = ProbShaper::new(target_page_size, page_size_stddev);
        let mut current_children = Vec::new();

        for child in current_level {
            let child_id = child.id.clone();
            current_children.push(child);

            // Check if we should create a branch page
            if shaper.is_complete(child_id.as_bytes()) && !current_children.is_empty() {
                let branch_meta = create_branch_from_children(&current_children)?;
                let branch_id =
                    write_branch_page(writer, &current_children, key_scheme, row_scheme)?;
                next_level.push(PageMetadata {
                    id: branch_id,
                    highest_key: branch_meta.highest_key,
                    depth: branch_meta.depth,
                    row_count: branch_meta.row_count,
                });
                current_children.clear();
                shaper = ProbShaper::new(target_page_size, page_size_stddev);
            }
        }

        // Don't forget the last branch page
        if !current_children.is_empty() {
            let branch_meta = create_branch_from_children(&current_children)?;
            let branch_id = write_branch_page(writer, &current_children, key_scheme, row_scheme)?;
            next_level.push(PageMetadata {
                id: branch_id,
                highest_key: branch_meta.highest_key,
                depth: branch_meta.depth,
                row_count: branch_meta.row_count,
            });
        }

        current_level = next_level;
    }

    // The single remaining page is our new root
    Ok(Some(current_level[0].id.clone()))
}
