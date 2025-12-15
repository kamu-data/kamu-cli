// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayData, ArrayRef, Int32Array, ListArray, StructArray};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Int32Type, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use opendatafabric_data_utils::data::DataFrameExt;
use opendatafabric_data_utils::testing::assert_dfs_equivalent;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Flat
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_assert_not_null_not_nullable() {
    let ctx = SessionContext::new();

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    false,
                )])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let actual = input.assert_columns_not_null(|_| true).unwrap();

    let expected = ctx
        .read_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    false,
                )])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        )
        .unwrap();

    assert_dfs_equivalent(expected.into(), actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_assert_not_null_nullable_valid() {
    let ctx = SessionContext::new();

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    true,
                )])),
                vec![Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)]))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let actual = input.assert_columns_not_null(|_| true).unwrap();

    let expected = ctx
        .read_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    false,
                )])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap(),
        )
        .unwrap();

    assert_dfs_equivalent(expected.into(), actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_assert_not_null_nullable_invalid() {
    let ctx = SessionContext::new();

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int32,
                    true,
                )])),
                vec![Arc::new(Int32Array::from(vec![Some(1), None, None]))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let actual = input.assert_columns_not_null(|_| true).unwrap();

    let res = actual.collect().await;

    assert_matches!(
        res,
        Err(::datafusion::error::DataFusionError::Execution(s))
        if s == "Column `value` contains 2 null values while none were expected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Nested
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
#[ignore = "Recursive struct filed null coersion is not yet supported"]
async fn test_assert_not_null_struct_valid() {
    let ctx = SessionContext::new();

    let field_req = Arc::new(Field::new("req", DataType::Int32, false));
    let field_opt = Arc::new(Field::new("opt", DataType::Int32, true));

    let schema = Arc::new(Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(Fields::from(vec![field_req.clone(), field_opt.clone()])),
        true,
    )]));

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StructArray::from(vec![
                    (
                        field_req.clone(),
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        field_opt,
                        Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3)])) as ArrayRef,
                    ),
                ]))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let field_opt_coerced = Arc::new(Field::new("opt", DataType::Int32, false));
    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "struct",
        DataType::Struct(Fields::from(vec![
            field_req.clone(),
            field_opt_coerced.clone(),
        ])),
        true,
    )]));

    let actual = input.coerce_columns_nullability(&expected_schema).unwrap();

    let expected = ctx
        .read_batch(
            RecordBatch::try_new(
                expected_schema,
                vec![Arc::new(StructArray::from(vec![
                    (
                        field_req,
                        Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef,
                    ),
                    (
                        field_opt_coerced,
                        Arc::new(Int32Array::from(vec![4, 5, 6])) as ArrayRef,
                    ),
                ]))],
            )
            .unwrap(),
        )
        .unwrap();

    assert_dfs_equivalent(expected.into(), actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_assert_not_null_list_elements_valid() {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "foo",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        false,
    )]));

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![
                        Some(vec![Some(0), Some(1), Some(2)]),
                        Some(vec![Some(3), Some(4), Some(5)]),
                    ],
                ))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "foo",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
        false,
    )]));

    let actual = input.coerce_columns_nullability(&expected_schema).unwrap();

    // Construct a value array
    let value_data = ArrayData::builder(DataType::Int32)
        .len(6)
        .add_buffer(arrow::buffer::Buffer::from_slice_ref([0, 1, 2, 3, 4, 5]))
        .build()
        .unwrap();

    let value_offsets = arrow::buffer::Buffer::from_slice_ref([0, 3, 6]);

    let list_data = ArrayData::builder(DataType::List(Arc::new(Field::new_list_field(
        DataType::Int32,
        false,
    ))))
    .len(2)
    .add_buffer(value_offsets.clone())
    .add_child_data(value_data.clone())
    .build()
    .unwrap();

    let expected = ctx
        .read_batch(
            RecordBatch::try_new(expected_schema, vec![Arc::new(ListArray::from(list_data))])
                .unwrap(),
        )
        .unwrap();

    assert_dfs_equivalent(expected.into(), actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_assert_not_null_list_elements_invalid() {
    let ctx = SessionContext::new();

    let schema = Arc::new(Schema::new(vec![Field::new(
        "foo",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
        false,
    )]));

    let input: DataFrameExt = ctx
        .read_batch(
            RecordBatch::try_new(
                schema,
                vec![Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(
                    vec![
                        Some(vec![Some(0), Some(1), Some(2)]),
                        Some(vec![Some(3), Some(4), None]),
                    ],
                ))],
            )
            .unwrap(),
        )
        .unwrap()
        .into();

    let expected_schema = Arc::new(Schema::new(vec![Field::new(
        "foo",
        DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false))),
        false,
    )]));

    let actual = input.coerce_columns_nullability(&expected_schema).unwrap();

    let res = actual.collect().await;

    assert_matches!(
        res,
        Err(::datafusion::error::DataFusionError::Execution(s))
        if s == "List column `foo` contains 1 null elements while none were expected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
