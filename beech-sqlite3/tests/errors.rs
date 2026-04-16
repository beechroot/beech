mod common;
use common::*;

// BUG: missing root file does not surface as an error through execute_batch.
#[test]
#[ignore]
fn missing_root_file_surfaces_error() {
    let tmp = tempfile::TempDir::new().unwrap();
    // No tree written — root file doesn't exist.
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    let sql = format!(
        "CREATE VIRTUAL TABLE tt USING beech('{}', 'unused', 'table')",
        tmp.path().display(),
    );
    let result = conn.execute_batch(&sql);
    assert!(result.is_err(), "should fail with missing root file");
}

// BUG: wrong table name does not surface as an error through execute_batch.
#[test]
#[ignore]
fn missing_table_in_transaction_surfaces_not_found() {
    let rows: Vec<_> = (0..5).map(|i| int_row(i, i as i32)).collect();
    let tmp = make_test_tree(rows, vec![0], "t");
    // The tree was written under table name "t" but we ask for "nonexistent".
    let conn = rusqlite::Connection::open_in_memory().unwrap();
    beech_sqlite3::create_beech_module(&conn).unwrap();
    let sql = format!(
        "CREATE VIRTUAL TABLE tt USING beech('{}', 'unused', 'nonexistent')",
        tmp.path().display(),
    );
    let result = conn.execute_batch(&sql);
    assert!(result.is_err(), "should fail when table name doesn't match");
}
