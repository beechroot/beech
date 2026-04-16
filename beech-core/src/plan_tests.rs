use super::*;
use crate::TableSchema;
use apache_avro::Schema;

fn table_with_keys(name: &str, key_parts: &[&str], all_cols: &[&str]) -> Table {
    let key_fields: String = key_parts
        .iter()
        .map(|n| format!(r#"{{"name": "{n}", "type": "long"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let row_fields: String = all_cols
        .iter()
        .map(|n| format!(r#"{{"name": "{n}", "type": "long"}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let key_scheme = Schema::parse_str(&format!(
        r#"{{"type": "record", "name": "{name}_key", "fields": [{key_fields}]}}"#
    ))
    .unwrap();
    let row_scheme = Schema::parse_str(&format!(
        r#"{{"type": "record", "name": "{name}_row", "fields": [{row_fields}]}}"#
    ))
    .unwrap();
    Table::new(
        1.into(),
        name.to_string(),
        None,
        TableSchema {
            key_scheme,
            row_scheme,
        },
    )
    .unwrap()
}

fn cc(column: i32, op: ConstraintOp) -> CandidateConstraint {
    CandidateConstraint { column, op }
}

#[test]
fn empty_candidates_yields_full_scan() {
    let table = table_with_keys("t", &["k0"], &["k0", "v"]);
    let plan = AccessPlan::select(&table, std::iter::empty(), 1_000_000);
    assert!(plan.search.is_empty());
    assert_eq!(plan.estimate.estimated_rows, 1_000_000);
}

#[test]
fn single_eq_on_key_part_zero() {
    let table = table_with_keys("t", &["k0"], &["k0", "v"]);
    let plan = AccessPlan::select(&table, [cc(0, ConstraintOp::Eq)], 1_000_000);
    assert_eq!(plan.search.len(), 1);
    assert_eq!(plan.search[0].key_part, 0);
    assert_eq!(plan.search[0].op, ConstraintOp::Eq);
    assert_eq!(plan.search[0].argv_index, 1);
    // Pinpoint: full single-part key with all equality.
    assert_eq!(plan.estimate.estimated_rows, 1);
}

#[test]
fn eq_on_non_key_column_is_dropped() {
    let table = table_with_keys("t", &["k0"], &["k0", "v"]);
    // column 1 ("v") is not a key part.
    let plan = AccessPlan::select(&table, [cc(1, ConstraintOp::Eq)], 1_000_000);
    assert!(plan.search.is_empty());
}

#[test]
fn unknown_op_is_dropped() {
    let table = table_with_keys("t", &["k0"], &["k0", "v"]);
    let plan = AccessPlan::select(&table, [cc(0, ConstraintOp::Unknown)], 1_000_000);
    assert!(plan.search.is_empty());
}

#[test]
fn eq_prefix_plus_range_on_next_part() {
    let table = table_with_keys("t", &["k0", "k1"], &["k0", "k1", "v"]);
    let plan = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Eq), cc(1, ConstraintOp::Gt)],
        1_000_000,
    );
    assert_eq!(plan.search.len(), 2);
    assert_eq!(plan.search[0].op, ConstraintOp::Eq);
    assert_eq!(plan.search[1].op, ConstraintOp::Gt);
    assert_eq!(plan.search[0].argv_index, 1);
    assert_eq!(plan.search[1].argv_index, 2);
}

#[test]
fn skipped_leading_key_part_yields_empty_search() {
    let table = table_with_keys("t", &["k0", "k1"], &["k0", "k1", "v"]);
    // Constraint only on key part 1, none on part 0 → no usable prefix.
    let plan = AccessPlan::select(&table, [cc(1, ConstraintOp::Eq)], 1_000_000);
    assert!(plan.search.is_empty());
}

#[test]
fn range_at_part_zero_then_stops() {
    let table = table_with_keys("t", &["k0", "k1"], &["k0", "k1", "v"]);
    // Range on part 0 + Eq on part 1: range stops the prefix walk.
    let plan = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Ge), cc(1, ConstraintOp::Eq)],
        1_000_000,
    );
    assert_eq!(plan.search.len(), 1);
    assert_eq!(plan.search[0].op, ConstraintOp::Ge);
}

#[test]
fn eq_preferred_over_range_at_same_part() {
    let table = table_with_keys("t", &["k0"], &["k0", "v"]);
    let plan = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Gt), cc(0, ConstraintOp::Eq)],
        1_000_000,
    );
    assert_eq!(plan.search.len(), 1);
    assert_eq!(plan.search[0].op, ConstraintOp::Eq);
}

#[test]
fn estimates_form_a_gradient() {
    let table = table_with_keys("t", &["k0", "k1"], &["k0", "k1", "v"]);
    let n = 1_000_000;

    let full_scan = AccessPlan::select(&table, std::iter::empty(), n);
    let prefix_eq = AccessPlan::select(&table, [cc(0, ConstraintOp::Eq)], n);
    let prefix_eq_plus_range = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Eq), cc(1, ConstraintOp::Gt)],
        n,
    );
    let pinpoint = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Eq), cc(1, ConstraintOp::Eq)],
        n,
    );

    assert!(pinpoint.estimate.estimated_cost < prefix_eq_plus_range.estimate.estimated_cost);
    assert!(prefix_eq_plus_range.estimate.estimated_cost < prefix_eq.estimate.estimated_cost);
    assert!(prefix_eq.estimate.estimated_cost < full_scan.estimate.estimated_cost);
}

#[test]
fn plan_round_trip_through_avro() {
    use apache_avro::schema::AvroSchema;
    use apache_avro::{from_avro_datum, to_avro_datum, to_value};

    let table = table_with_keys("t", &["k0", "k1"], &["k0", "k1", "v"]);
    let plan = AccessPlan::select(
        &table,
        [cc(0, ConstraintOp::Eq), cc(1, ConstraintOp::Gt)],
        500_000,
    );

    let schema = AccessPlan::get_schema();
    let encoded = to_avro_datum(&schema, to_value(&plan).unwrap()).unwrap();
    let decoded_value = from_avro_datum(&schema, &mut std::io::Cursor::new(encoded), None).unwrap();
    let decoded: AccessPlan = apache_avro::from_value(&decoded_value).unwrap();

    assert_eq!(plan.table_id, decoded.table_id);
    assert_eq!(plan.search.len(), decoded.search.len());
    assert_eq!(plan.preserves_order, decoded.preserves_order);
    assert_eq!(
        plan.estimate.estimated_rows,
        decoded.estimate.estimated_rows
    );
    for (a, b) in plan.search.iter().zip(decoded.search.iter()) {
        assert_eq!(a.key_part, b.key_part);
        assert_eq!(a.column, b.column);
        assert_eq!(a.op, b.op);
        assert_eq!(a.argv_index, b.argv_index);
    }
}
