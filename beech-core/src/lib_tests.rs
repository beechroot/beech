#![allow(unused_imports)]
use super::*;
use apache_avro::Schema;
use apache_avro::types::Value;

fn int_schema(name: &str, field_names: &[&str]) -> Schema {
    let fields: Vec<String> = field_names
        .iter()
        .map(|f| format!(r#"{{"name":"{}","type":"int"}}"#, f))
        .collect();
    let s = format!(
        r#"{{"type":"record","name":"{}","fields":[{}]}}"#,
        name,
        fields.join(",")
    );
    Schema::parse_str(&s).unwrap()
}

#[test]
fn id_from_hex_round_trip() {
    let hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let id = Id::from_hex(hex).unwrap();
    // Display uses hex; compare case-insensitively because uppercase is valid too.
    assert_eq!(format!("{}", id).to_lowercase(), hex);
    let again = Id::from_hex(&format!("{}", id)).unwrap();
    assert_eq!(id, again);
}

#[test]
fn id_from_hex_rejects_odd_length() {
    let err = Id::from_hex("abc");
    assert!(err.is_err());
}

#[test]
fn id_from_hex_rejects_non_hex() {
    let bad = "zz23456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    let err = Id::from_hex(bad);
    assert!(err.is_err());
}

#[test]
fn key_ordering_lexicographic() {
    let a: Key = vec![Value::Int(1), Value::Int(2)];
    let b: Key = vec![Value::Int(1), Value::Int(3)];
    let c: Key = vec![Value::Int(2), Value::Int(0)];
    assert_eq!(a.compare_key(&b), std::cmp::Ordering::Less);
    assert_eq!(b.compare_key(&a), std::cmp::Ordering::Greater);
    assert_eq!(a.compare_key(&a.clone()), std::cmp::Ordering::Equal);
    assert_eq!(b.compare_key(&c), std::cmp::Ordering::Less);
}

#[test]
fn key_ordering_handles_mixed_types() {
    // Int vs Long should compare numerically, not bail out.
    let a: Key = vec![Value::Int(5)];
    let b: Key = vec![Value::Long(10)];
    assert_eq!(a.compare_key(&b), std::cmp::Ordering::Less);

    // Null should be less than anything.
    let n: Key = vec![Value::Null];
    let x: Key = vec![Value::Int(0)];
    assert_eq!(n.compare_key(&x), std::cmp::Ordering::Less);
}

#[test]
fn table_new_rejects_non_record_schemas() {
    let schema = TableSchema {
        key_scheme: Schema::Int,
        row_scheme: Schema::Int,
    };
    let result = Table::new(1i64.into(), "t".to_string(), None, schema);
    assert!(result.is_err());
}

#[test]
fn table_new_rejects_unsupported_field_type() {
    // Array-of-int is not in the supported primitive set.
    let row_scheme = Schema::parse_str(
        r#"{"type":"record","name":"r","fields":[{"name":"xs","type":{"type":"array","items":"int"}}]}"#,
    )
    .unwrap();
    let key_scheme = int_schema("k", &["xs"]);
    let schema = TableSchema {
        key_scheme,
        row_scheme,
    };
    let result = Table::new(1i64.into(), "t".to_string(), None, schema);
    assert!(result.is_err());
}

fn two_int_table() -> Table {
    let schema = TableSchema {
        key_scheme: int_schema("k", &["a"]),
        row_scheme: int_schema("r", &["a", "b"]),
    };
    Table::new(1i64.into(), "t".to_string(), None, schema).unwrap()
}

#[test]
fn node_keys_internal_returns_separators() {
    let keys = vec![vec![Value::Int(10)], vec![Value::Int(20)]];
    let node = Node::Internal(InternalNode {
        keys: keys.clone(),
        children: vec![1i64.into(), 2i64.into(), 3i64.into()],
        subtree_height: 1,
        subtree_row_count: 30,
    });
    assert_eq!(node.keys(), keys.as_slice());
    assert_eq!(node.len(), 3);
    assert_eq!(node.last_slot_index(), Some(2));
    assert!(node.is_internal());
    assert!(!node.is_leaf());
    assert_eq!(node.depth(), 1);
    assert_eq!(node.row_count(), 30);
}

#[test]
fn node_keys_leaf_returns_one_per_entry() {
    let keys = vec![
        vec![Value::Int(1)],
        vec![Value::Int(2)],
        vec![Value::Int(3)],
    ];
    let entries: Vec<LeafEntry> = vec![
        LeafEntry {
            row: (10, vec![Value::Int(1), Value::Int(100)]),
        },
        LeafEntry {
            row: (11, vec![Value::Int(2), Value::Int(200)]),
        },
        LeafEntry {
            row: (12, vec![Value::Int(3), Value::Int(300)]),
        },
    ];
    let node = Node::Leaf(LeafNode {
        keys: keys.clone(),
        entries,
    });
    assert_eq!(node.keys(), keys.as_slice());
    assert_eq!(node.keys().len(), 3);
    assert_eq!(node.len(), 3);
    assert!(node.is_leaf());
    assert!(!node.is_internal());
    assert_eq!(node.depth(), 0);
    assert_eq!(node.row_count(), 3);
}

#[test]
fn leaf_entry_key_derives_from_table() {
    let table = two_int_table();
    let entry = LeafEntry {
        row: (42, vec![Value::Int(7), Value::Int(99)]),
    };
    assert_eq!(entry.row_id(), 42);
    assert_eq!(entry.values(), &[Value::Int(7), Value::Int(99)]);
    assert_eq!(entry.key(&table), vec![Value::Int(7)]);
}

#[test]
fn node_owned_key_at_returns_correct_key() {
    let table = two_int_table();
    let leaf = Node::Leaf(LeafNode {
        keys: vec![vec![Value::Int(5)], vec![Value::Int(6)]],
        entries: vec![
            LeafEntry {
                row: (1, vec![Value::Int(5), Value::Int(50)]),
            },
            LeafEntry {
                row: (2, vec![Value::Int(6), Value::Int(60)]),
            },
        ],
    });
    assert_eq!(leaf.owned_key_at(&table, 0), Some(vec![Value::Int(5)]));
    assert_eq!(leaf.owned_key_at(&table, 1), Some(vec![Value::Int(6)]));
    assert_eq!(leaf.owned_key_at(&table, 2), None);

    let internal = Node::Internal(InternalNode {
        keys: vec![vec![Value::Int(100)]],
        children: vec![1i64.into(), 2i64.into()],
        subtree_height: 1,
        subtree_row_count: 10,
    });
    assert_eq!(
        internal.owned_key_at(&table, 0),
        Some(vec![Value::Int(100)])
    );
}

#[test]
fn last_slot_index_handles_empty() {
    let empty_leaf = Node::Leaf(LeafNode {
        keys: vec![],
        entries: vec![],
    });
    assert_eq!(empty_leaf.last_slot_index(), None);
    assert!(empty_leaf.is_empty());

    let empty_internal = Node::Internal(InternalNode {
        keys: vec![],
        children: vec![],
        subtree_height: 1,
        subtree_row_count: 0,
    });
    assert_eq!(empty_internal.last_slot_index(), None);
    assert!(empty_internal.is_empty());
}
