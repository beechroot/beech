//! TransactionBuffer tests.
//!
//! Correctness bar: applying a sequence of Changes to a TransactionBuffer
//! must produce the same virtual_root Id as a fresh batch build of the
//! same final row-set (prolly-canonical invariant).

use apache_avro::types::Value;
use beech_test_fixtures::{build_simple_table, MemoryStore};
use beech_write::transaction::TransactionBuffer;
use beech_write::{write_rows_to_prolly_tree, Change};

fn int_record(k: i32, v: i32) -> Value {
    Value::Record(vec![
        ("k".to_string(), Value::Int(k)),
        ("v".to_string(), Value::Int(v)),
    ])
}

fn int_row(i: i64, v: i32) -> (i64, Value) {
    (i, int_record(i as i32, v))
}

const TARGET: usize = 64;
const STDDEV: usize = 16;

/// Build the tree that `rows` would produce via a fresh batch build,
/// and return its root-node Id.
fn batch_root(rows: Vec<(i64, Value)>) -> beech_core::Id {
    let store = MemoryStore::new();
    let mut writer = store.writer();
    write_rows_to_prolly_tree(
        &mut writer,
        "t".to_string(),
        vec![0],
        rows,
        TARGET,
        STDDEV,
        None,
    )
    .unwrap();
    // read-back via the store to get the table and its root
    use beech_core::NodeSource;
    let source = store.node_source();
    let root = source.get_root().unwrap();
    let txn = source.get_transaction(&root.id).unwrap();
    let table = source.get_table(&txn, "t").unwrap();
    table.root.clone().expect("non-empty tree")
}

#[test]
fn apply_insert_matches_batch() {
    // Seed the tree with 0..10. Then via TransactionBuffer, insert a new
    // row with key 100. The virtual_root should equal the batch-built
    // root of 0..10 plus the extra row.
    let seed: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(100)],
        row_id: 100,
        record: vec![Value::Int(100), Value::Int(900)],
    })
    .unwrap();

    let mut final_rows = seed;
    final_rows.push((100, int_record(100, 900)));
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_update_matches_batch() {
    let seed: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Update {
        key: vec![Value::Int(5)],
        row_id: 5,
        record: vec![Value::Int(5), Value::Int(7777)],
    })
    .unwrap();

    let mut final_rows: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    final_rows[5] = (5, int_record(5, 7777));
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_delete_matches_batch() {
    let seed: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Delete {
        key: vec![Value::Int(3)],
    })
    .unwrap();

    let final_rows: Vec<_> = (0..10)
        .filter(|i| *i != 3)
        .map(|i| int_row(i, i as i32))
        .collect();
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_many_inserts_matches_batch() {
    // Seed with 0,2,4,...,198 (evens); insert all odds 1,3,...,199 via
    // TransactionBuffer. Final tree should equal batch build of 0..200.
    let seed: Vec<_> = (0..200).step_by(2).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    for i in (1..200).step_by(2) {
        buf.apply(Change::Insert {
            key: vec![Value::Int(i as i32)],
            row_id: i,
            record: vec![Value::Int(i as i32), Value::Int(i as i32)],
        })
        .unwrap();
    }

    let final_rows: Vec<_> = (0..200).map(|i| int_row(i, i as i32)).collect();
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_interleaved_matches_batch() {
    let seed: Vec<_> = (0..50).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(100)],
        row_id: 100,
        record: vec![Value::Int(100), Value::Int(0)],
    })
    .unwrap();
    buf.apply(Change::Update {
        key: vec![Value::Int(10)],
        row_id: 10,
        record: vec![Value::Int(10), Value::Int(999)],
    })
    .unwrap();
    buf.apply(Change::Delete {
        key: vec![Value::Int(20)],
    })
    .unwrap();
    buf.apply(Change::Insert {
        key: vec![Value::Int(-5)],
        row_id: 200,
        record: vec![Value::Int(-5), Value::Int(-5)],
    })
    .unwrap();

    let mut final_rows: Vec<_> = (0..50).map(|i| int_row(i, i as i32)).collect();
    final_rows[10] = (10, int_record(10, 999));
    final_rows.retain(|(r, _)| *r != 20);
    final_rows.push((100, int_record(100, 0)));
    final_rows.push((200, int_record(-5, -5)));
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_full_delete_returns_empty_root() {
    let seed: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    for i in 0..5 {
        buf.apply(Change::Delete {
            key: vec![Value::Int(i)],
        })
        .unwrap();
    }
    assert_eq!(buf.virtual_root(), None);
}

#[test]
fn apply_duplicate_insert_errors() {
    let seed: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    let err = buf
        .apply(Change::Insert {
            key: vec![Value::Int(2)],
            row_id: 99,
            record: vec![Value::Int(2), Value::Int(999)],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        beech_core::BeechError::Domain(beech_core::DomainError::DuplicateKey { .. })
    ));
}

#[test]
fn apply_missing_update_errors() {
    let seed: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    let err = buf
        .apply(Change::Update {
            key: vec![Value::Int(99)],
            row_id: 99,
            record: vec![Value::Int(99), Value::Int(0)],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        beech_core::BeechError::Domain(beech_core::DomainError::KeyNotFound { .. })
    ));
}

#[test]
fn apply_append_at_end_matches_batch() {
    // Key range 0..100; append key 200. Affects only the last leaf.
    let seed: Vec<_> = (0..100).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(200)],
        row_id: 200,
        record: vec![Value::Int(200), Value::Int(200)],
    })
    .unwrap();

    let mut final_rows = seed;
    final_rows.push((200, int_record(200, 200)));
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn apply_large_tree_single_update_matches_batch() {
    // Large enough tree to have a multi-level structure; update one row
    // in the middle. Exercises cascade through multiple levels.
    let n = 1_000i64;
    let seed: Vec<_> = (0..n).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Update {
        key: vec![Value::Int(500)],
        row_id: 500,
        record: vec![Value::Int(500), Value::Int(9_999_999)],
    })
    .unwrap();

    let mut final_rows: Vec<_> = (0..n).map(|i| int_row(i, i as i32)).collect();
    final_rows[500] = (500, int_record(500, 9_999_999));
    let expected = batch_root(final_rows);
    assert_eq!(buf.virtual_root(), Some(&expected));
}

#[test]
fn commit_writes_nodes_readable_by_new_source() {
    // Seed, open a TransactionBuffer against the seeded store, apply a
    // change, commit through the writer backed by the same store, and
    // read the virtual_root through the store's NodeSource — we should
    // reach the modified leaf without the overlay being involved.
    let seed: Vec<_> = (0..40).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();

    let mut buf = TransactionBuffer::new(source, table.clone(), TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(100)],
        row_id: 100,
        record: vec![Value::Int(100), Value::Int(9999)],
    })
    .unwrap();
    let expected_root = buf.virtual_root().cloned().expect("non-empty");

    // Writer backed by the same store so committed nodes become visible
    // through the node_source.
    let mut writer = store.writer();
    let new_root = buf.commit(&mut writer).unwrap();
    assert_eq!(new_root, Some(expected_root.clone()));

    // Reach the new root through the bare (no-overlay) node_source.
    let source2 = store.node_source();
    use beech_core::NodeSource;
    let node = source2.get_node(&expected_root, &table.schema).unwrap();
    // subtree_row_count should be 41 (40 seed + 1 new)
    assert_eq!(node.row_count(), 41);
}

#[test]
fn commit_roundtrip_matches_batch() {
    // Full pipeline: seed → TransactionBuffer → apply changes → commit →
    // re-read tree → compare walked row set to batch build of final rows.
    let seed: Vec<_> = (0..30).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) =
        build_simple_table("t", seed.clone(), vec![0], TARGET, STDDEV).unwrap();

    let mut buf = TransactionBuffer::new(source, table.clone(), TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(500)],
        row_id: 500,
        record: vec![Value::Int(500), Value::Int(500)],
    })
    .unwrap();
    buf.apply(Change::Delete {
        key: vec![Value::Int(15)],
    })
    .unwrap();
    buf.apply(Change::Update {
        key: vec![Value::Int(7)],
        row_id: 7,
        record: vec![Value::Int(7), Value::Int(777)],
    })
    .unwrap();

    let mut writer = store.writer();
    let new_root = buf.commit(&mut writer).unwrap().expect("non-empty");

    // Independently compute the expected root.
    let mut final_rows = seed;
    final_rows[7] = (7, int_record(7, 777));
    final_rows.retain(|(r, _)| *r != 15);
    final_rows.push((500, int_record(500, 500)));
    let expected = batch_root(final_rows);
    assert_eq!(new_root, expected);
}

#[test]
fn commit_empty_tree_returns_none_root() {
    let seed: Vec<_> = (0..3).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    for i in 0..3 {
        buf.apply(Change::Delete {
            key: vec![Value::Int(i)],
        })
        .unwrap();
    }
    let mut writer = store.writer();
    let new_root = buf.commit(&mut writer).unwrap();
    assert_eq!(new_root, None);
}

#[test]
fn rollback_does_not_write() {
    let seed: Vec<_> = (0..10).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();
    let seed_file_count = store.file_count();

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    buf.apply(Change::Insert {
        key: vec![Value::Int(99)],
        row_id: 99,
        record: vec![Value::Int(99), Value::Int(99)],
    })
    .unwrap();
    buf.rollback();

    // Dropping the buffer without commit means the store file count is
    // unchanged — no .bch files leaked.
    assert_eq!(store.file_count(), seed_file_count);
}

#[test]
fn apply_missing_delete_errors() {
    let seed: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let (store, source, table) = build_simple_table("t", seed, vec![0], TARGET, STDDEV).unwrap();
    drop(store);

    let mut buf = TransactionBuffer::new(source, table, TARGET, STDDEV);
    let err = buf
        .apply(Change::Delete {
            key: vec![Value::Int(99)],
        })
        .unwrap_err();
    assert!(matches!(
        err,
        beech_core::BeechError::Domain(beech_core::DomainError::KeyNotFound { .. })
    ));
}
