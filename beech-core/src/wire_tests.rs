#![allow(unused_imports)]
use super::*;

fn init_logger() {
    // do only once
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        simple_logger::SimpleLogger::new().env().init().unwrap();
    });
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, AvroSchema)]
struct MyRecord {
    x: i32,
    y: i32,
}
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, AvroSchema)]
struct MyKey {
    x: i32,
}

#[test]
fn test_page_roundtrip() {
    init_logger();
    let keys = vec![
        vec![Value::Int(1)],
        vec![Value::Int(2)],
        vec![Value::Int(3)],
    ];
    let rows = vec![
        (1000, vec![Value::Int(1), Value::Int(100)]),
        (1001, vec![Value::Int(2), Value::Int(200)]),
        (1002, vec![Value::Int(3), Value::Int(300)]),
    ];
    let leaf = Node::Leaf(LeafNode {
        keys: keys.clone(),
        entries: rows.into_iter().map(|row| LeafEntry { row }).collect(),
    });
    let branch = Node::Internal(InternalNode {
        keys: keys.clone(),
        children: vec![10001.into(), 10002.into(), 10003.into(), 10004.into()],
        subtree_height: 1,
        subtree_row_count: 3,
    });

    for page in [leaf, branch] {
        let schema = TableSchema {
            key_scheme: MyKey::get_schema(),
            row_scheme: MyRecord::get_schema(),
        };
        let encoded = encode_page(&page, 9999.into(), &schema).unwrap();
        let decoded = decode_page(&mut Cursor::new(encoded), &schema).unwrap();
        assert_eq!(page, decoded);
    }
}

#[test]
fn test_table_roundtrip() {
    init_logger();
    let schema = TableSchema {
        key_scheme: Schema::parse_str(
            r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": "int"}] }"#,
        )
        .unwrap(),
        row_scheme: Schema::parse_str(
            r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": "int"}] }"#,
        )
        .unwrap(),
    };
    let table = Table::new(999.into(), "test".to_string(), None, schema).unwrap();
    let encoded = encode_table(&table, 1234567890).unwrap();
    let decoded = decode_table(&mut Cursor::new(encoded)).unwrap();
    assert_eq!(table, decoded);
}

#[test]
fn decode_page_truncated_input_errors_cleanly() {
    let keys = vec![vec![Value::Int(1)], vec![Value::Int(2)]];
    let rows = vec![
        (100, vec![Value::Int(1), Value::Int(10)]),
        (101, vec![Value::Int(2), Value::Int(20)]),
    ];
    let leaf = Node::Leaf(LeafNode {
        keys: keys.clone(),
        entries: rows.into_iter().map(|row| LeafEntry { row }).collect(),
    });
    let schema = TableSchema {
        key_scheme: MyKey::get_schema(),
        row_scheme: MyRecord::get_schema(),
    };
    let encoded = encode_page(&leaf, 1i64.into(), &schema).unwrap();
    // Lop off the back half of the payload and make sure decode errors
    // rather than panicking.
    let truncated = &encoded[..encoded.len() / 2];
    let mut reader = Cursor::new(truncated.to_vec());
    let result = decode_page(&mut reader, &schema);
    assert!(result.is_err(), "decoding truncated input should error");
}

#[test]
fn decode_page_wrong_schema_errors_cleanly() {
    let keys = vec![vec![Value::Int(1)]];
    let rows = vec![(100, vec![Value::Int(1), Value::Int(10)])];
    let leaf = Node::Leaf(LeafNode {
        keys,
        entries: rows.into_iter().map(|row| LeafEntry { row }).collect(),
    });
    let encode_schema = TableSchema {
        key_scheme: MyKey::get_schema(),
        row_scheme: MyRecord::get_schema(),
    };
    let encoded = encode_page(&leaf, 1i64.into(), &encode_schema).unwrap();

    // Decode with a row schema that has an extra string field — should not
    // panic; must return an error instead.
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq, AvroSchema)]
    struct OtherRecord {
        x: i32,
        y: i32,
        z: String,
    }
    let wrong_schema = TableSchema {
        key_scheme: MyKey::get_schema(),
        row_scheme: OtherRecord::get_schema(),
    };
    let mut reader = Cursor::new(encoded);
    let result = decode_page(&mut reader, &wrong_schema);
    assert!(
        result.is_err(),
        "decoding with mismatched schema should error"
    );
}

#[test]
fn test_simple_nullable_field() {
    // ATTENTION: ["null", "int"] order matters!
    let schema = Schema::parse_str(r#"{ "type": "record", "name": "aaa", "fields": [{"name": "x", "type": ["null", "int"]}] }"#).unwrap();
    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Foo {
        x: Option<i32>,
    }
    let foo = Foo { x: Some(1) };
    let encoded = to_avro_datum(&schema, to_value(&foo).unwrap()).unwrap();
    let decoded = from_avro_datum(&schema, &mut Cursor::new(encoded), None).unwrap();
    assert_eq!(&from_value::<Foo>(&decoded).unwrap(), &foo);
}
