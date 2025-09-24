// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::sync::Arc;

use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use opendatafabric_data_utils::data::DataFrameExt;
use opendatafabric_data_utils::testing::{assert_dfs_equal, assert_dfs_equivalent};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_assert_dfs_equal_success() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed: `(left == right)`

\u{1b}[1mDiff\u{1b}[0m \u{1b}[31m< left\u{1b}[0m / \u{1b}[32mright >\u{1b}[0m :
 +-----------+------------+
 | city      | population |
 +-----------+------------+
\u{1b}[31m<| vancouver | 1          |\u{1b}[0m
 | seattle   | 2          |
\u{1b}[32m>| vancouver | 1          |\u{1b}[0m
 | kyiv      | 3          |
 +-----------+------------+

")]
async fn test_assert_dfs_equal_fails_order() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("seattle", 2), ("vancouver", 1), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed: `(left == right)`

\u{1b}[1mDiff\u{1b}[0m \u{1b}[31m< left\u{1b}[0m / \u{1b}[32mright >\u{1b}[0m :
 +-----------+------------+
 | city      | population |
 +-----------+------------+
 | vancouver | 1          |
 | seattle   | 2          |
 | kyiv      | 3          |
\u{1b}[32m>| odessa    | 4          |\u{1b}[0m
 +-----------+------------+

")]
async fn test_assert_dfs_equal_fails_count() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(
        &ctx,
        [("vancouver", 1), ("seattle", 2), ("kyiv", 3), ("odessa", 4)],
    );
    assert_dfs_equal(a, b).await;
}

#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed: `(left == right)`

\u{1b}[1mDiff\u{1b}[0m \u{1b}[31m< left\u{1b}[0m / \u{1b}[32mright >\u{1b}[0m :
 +-----------+------------+
 | city      | population |
 +-----------+------------+
 | vancouver | 1          |
\u{1b}[31m<| seattle   | \u{1b}[0m\u{1b}[1;48;5;52;31m2\u{1b}[0m\u{1b}[31m          |\u{1b}[0m
\u{1b}[32m>| seattle   | \u{1b}[0m\u{1b}[1;48;5;22;32m1\u{1b}[0m\u{1b}[32m          |\u{1b}[0m
 | kyiv      | 3          |
 +-----------+------------+

")]
async fn test_assert_dfs_equal_fails_column() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("vancouver", 1), ("seattle", 1), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_assert_dfs_equivalent_success() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("kyiv", 3), ("vancouver", 1), ("seattle", 2)]);
    assert_dfs_equivalent(a, b, true, true, true).await;
}

#[test_log::test(tokio::test)]
#[should_panic(expected = "assertion failed: `(left == right)`

\u{1b}[1mDiff\u{1b}[0m \u{1b}[31m< left\u{1b}[0m / \u{1b}[32mright >\u{1b}[0m :
 +-----------+------------+
 | city      | population |
 +-----------+------------+
\u{1b}[31m<| vancouver | \u{1b}[0m\u{1b}[1;48;5;52;31m1\u{1b}[0m\u{1b}[31m          |\u{1b}[0m
\u{1b}[32m>| vancouver | \u{1b}[0m\u{1b}[1;48;5;22;32m2\u{1b}[0m\u{1b}[32m          |\u{1b}[0m
 | seattle   | 2          |
 | kyiv      | 3          |
 +-----------+------------+

")]
async fn test_assert_dfs_equivalent_fails_column() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("kyiv", 3), ("vancouver", 2), ("seattle", 2)]);
    assert_dfs_equivalent(a, b, true, true, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut city = Vec::new();
    let mut population = Vec::new();

    for (c, p) in rows {
        city.push(c.into());
        population.push(p);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::StringArray::from(city)),
            Arc::new(array::Int64Array::from(population)),
        ],
    )
    .unwrap();

    ctx.read_batch(batch).unwrap().into()
}
