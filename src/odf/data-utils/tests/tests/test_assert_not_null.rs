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

use datafusion::arrow::array::Int32Array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use opendatafabric_data_utils::data::DataFrameExt;
use opendatafabric_data_utils::testing::assert_dfs_equivalent;

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

    let actual = input.assert_collumns_not_null(|_| true).unwrap();

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

    let actual = input.assert_collumns_not_null(|_| true).unwrap();

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

    let actual = input.assert_collumns_not_null(|_| true).unwrap();

    let res = actual.collect().await;

    assert_matches!(
        res,
        Err(::datafusion::error::DataFusionError::Execution(s))
        if s == "Column value contains 2 null values while none were expected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
