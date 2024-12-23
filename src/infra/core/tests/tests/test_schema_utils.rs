// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::parquet::basic::{ConvertedType, LogicalType, Repetition, Type as PhysicalType};
use datafusion::parquet::schema::types::Type;
use odf::utils::schema::format::write_schema_parquet_json;

#[test]
fn test_write_schema_parquet_json_escaping() {
    let f1 = Type::primitive_type_builder("a \" b", PhysicalType::INT32)
        .build()
        .unwrap();
    let fields = vec![Arc::new(f1)];
    let message = Type::group_type_builder("schema")
        .with_fields(fields)
        .with_id(Some(1))
        .build()
        .unwrap();

    let mut buf = Vec::new();
    write_schema_parquet_json(&mut buf, &message).unwrap();

    println!("{}", std::str::from_utf8(&buf).unwrap());

    let actual: serde_json::Value = serde_json::from_slice(&buf).unwrap();

    let expected = serde_json::json!({
        "name": "schema",
        "type": "struct",
        "fields": [{
            "name": "a \" b",
            "repetition": "OPTIONAL",
            "type": "INT32",
        }]
    });

    assert_eq!(actual, expected);
}

#[test]
fn test_write_schema_parquet_json_group() {
    let f1 = Type::primitive_type_builder("f1", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .with_converted_type(ConvertedType::INT_32)
        .with_id(Some(0))
        .build();
    let f2 = Type::primitive_type_builder("f2", PhysicalType::BYTE_ARRAY)
        .with_converted_type(ConvertedType::UTF8)
        .with_id(Some(1))
        .build();
    let f3 = Type::primitive_type_builder("f3", PhysicalType::BYTE_ARRAY)
        .with_logical_type(Some(LogicalType::String))
        .with_id(Some(1))
        .build();
    let f4 = Type::primitive_type_builder("f4", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_repetition(Repetition::REPEATED)
        .with_converted_type(ConvertedType::INTERVAL)
        .with_length(12)
        .with_id(Some(2))
        .build();
    let f5 = Type::primitive_type_builder("f5", PhysicalType::FIXED_LEN_BYTE_ARRAY)
        .with_length(9)
        .with_converted_type(ConvertedType::DECIMAL)
        .with_precision(19)
        .with_scale(4)
        .build();
    let struct_fields = vec![
        Arc::new(f1.unwrap()),
        Arc::new(f2.unwrap()),
        Arc::new(f3.unwrap()),
    ];
    let field = Type::group_type_builder("field")
        .with_repetition(Repetition::OPTIONAL)
        .with_fields(struct_fields)
        .with_id(Some(1))
        .build()
        .unwrap();
    let fields = vec![
        Arc::new(field),
        Arc::new(f4.unwrap()),
        Arc::new(f5.unwrap()),
    ];
    let message = Type::group_type_builder("schema")
        .with_fields(fields)
        .with_id(Some(2))
        .build()
        .unwrap();

    let mut buf = Vec::new();
    write_schema_parquet_json(&mut buf, &message).unwrap();
    let actual: serde_json::Value = serde_json::from_slice(&buf).unwrap();

    let expected = serde_json::json!({
        "name": "schema",
        "type": "struct",
        "fields": [{
            "name": "field",
            "type": "struct",
            "repetition": "OPTIONAL",
            "fields": [{
                "name": "f1",
                "repetition": "REQUIRED",
                "type": "INT32",
                "logicalType": "INT_32"
            }, {
                "name": "f2",
                "repetition": "OPTIONAL",
                "type": "BYTE_ARRAY",
                "logicalType": "UTF8"
            }, {
                "name": "f3",
                "repetition": "OPTIONAL",
                "type": "BYTE_ARRAY",
                "logicalType": "STRING"
            }]
        }, {
            "name": "f4",
            "repetition": "REPEATED",
            "type": "FIXED_LEN_BYTE_ARRAY(12)",
            "logicalType": "INTERVAL"
        }, {
            "name": "f5",
            "repetition": "OPTIONAL",
            "type": "FIXED_LEN_BYTE_ARRAY(9)",
            "logicalType": "DECIMAL(19,4)"
        }]
    });

    assert_eq!(actual, expected);
}
