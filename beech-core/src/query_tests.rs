#![allow(unused_imports)]
use super::*;
use crate::{Table, TableSchema};
use apache_avro::types::Value;

// Cursor-level tests that need a populated tree live in
// `beech-test-fixtures/tests/cursor.rs`: beech-core cannot dev-depend on
// the fixtures crate without a cargo dep cycle (fixtures depends on
// beech-write, which depends on beech-core).

#[test]
fn constraint_new_starts_without_index() {
    let c = Constraint::new(3, ConstraintOp::Eq);
    assert_eq!(c.column, 3);
    assert_eq!(c.op, ConstraintOp::Eq);
    assert!(!c.has_index);
}

#[test]
fn constraint_op_is_copy_and_eq() {
    let a = ConstraintOp::Lt;
    let b = a; // Copy
    assert_eq!(a, b);
    assert_ne!(a, ConstraintOp::Gt);
}

#[test]
fn ord_value_null_is_less_than_anything() {
    let n = Value::Null;
    let i = Value::Int(0);
    assert!(OrdValue(&n) < OrdValue(&i));
    assert!(OrdValue(&i) > OrdValue(&n));
}

#[test]
fn ord_value_mixed_numeric_comparison() {
    // Int vs Long should compare numerically, not by discriminant.
    let i = Value::Int(5);
    let l = Value::Long(10);
    assert!(OrdValue(&i) < OrdValue(&l));

    let f = Value::Float(1.5);
    let d = Value::Double(2.5);
    assert!(OrdValue(&f) < OrdValue(&d));
}

#[test]
fn ord_value_equal_values_compare_equal() {
    let a = Value::Int(42);
    let b = Value::Int(42);
    assert_eq!(OrdValue(&a), OrdValue(&b));
}

#[test]
fn cursor_new_is_eof() {
    use apache_avro::Schema;
    let schema = TableSchema {
        key_scheme: Schema::parse_str(
            r#"{"type":"record","name":"k","fields":[{"name":"k","type":"int"}]}"#,
        )
        .unwrap(),
        row_scheme: Schema::parse_str(
            r#"{"type":"record","name":"r","fields":[{"name":"k","type":"int"}]}"#,
        )
        .unwrap(),
    };
    let table = Table::new(1i64.into(), "t".into(), None, schema).unwrap();
    let cursor = Cursor::new(&table);
    assert!(cursor.eof());
    assert_eq!(cursor.depth(), 0);
    assert!(cursor.current().is_none());
}
