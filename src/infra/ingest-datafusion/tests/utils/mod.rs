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
use datafusion::common::DFSchema;
use datafusion::prelude::*;
use datafusion::sql::TableReference;
use odf::utils::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn assert_dfs_equal(lhs: DataFrameExt, rhs: DataFrameExt) {
    pretty_assertions::assert_eq!(lhs.schema(), rhs.schema());

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(RecordBatch::num_rows).sum();
    let rhs_count: usize = rhs_batches.iter().map(RecordBatch::num_rows).sum();

    // This is a workaround for the situation where collect returns an empty vec vs.
    // a vec containing an empty batch
    if lhs_count == 0 && rhs_count == 0 {
        return;
    }

    let lhs_str = datafusion::arrow::util::pretty::pretty_format_batches(&lhs_batches)
        .unwrap()
        .to_string();

    let rhs_str = datafusion::arrow::util::pretty::pretty_format_batches(&rhs_batches)
        .unwrap()
        .to_string();

    pretty_assertions::assert_eq!(lhs_str, rhs_str);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Checks for equivalence ignoring the order of rows and nullability of columns
pub async fn assert_dfs_equivalent(
    lhs: DataFrameExt,
    rhs: DataFrameExt,
    ignore_order: bool,
    ignore_nullability: bool,
    ignore_qualifiers: bool,
) {
    let (lhs, rhs) = if !ignore_order {
        (lhs, rhs)
    } else {
        let lhs_cols = lhs
            .schema()
            .fields()
            .iter()
            .map(|f| col(Column::from_name(f.name())).sort(false, true))
            .collect();

        let rhs_cols = rhs
            .schema()
            .fields()
            .iter()
            .map(|f| col(Column::from_name(f.name())).sort(false, true))
            .collect();

        (lhs.sort(lhs_cols).unwrap(), rhs.sort(rhs_cols).unwrap())
    };

    let map_field =
        |(q, f): (Option<&TableReference>, &Arc<Field>)| -> (Option<TableReference>, Arc<Field>) {
            let q = q.cloned();
            let f = f.clone();
            let (q, f) = if !ignore_qualifiers {
                (q, f)
            } else {
                (None, f)
            };
            let f = if !ignore_nullability {
                f
            } else {
                Arc::new(f.as_ref().clone().with_nullable(true))
            };
            (q, f)
        };

    let lhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        qualified_fields(lhs.schema()).map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    let rhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        qualified_fields(rhs.schema()).map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(lhs_schema_stripped, rhs_schema_stripped);

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(RecordBatch::num_rows).sum();
    let rhs_count: usize = rhs_batches.iter().map(RecordBatch::num_rows).sum();

    // This is a workaround for the situation where collect returns an empty vec vs.
    // a vec containing an empty batch
    if lhs_count == 0 && rhs_count == 0 {
        return;
    }

    let lhs_str = datafusion::arrow::util::pretty::pretty_format_batches(&lhs_batches)
        .unwrap()
        .to_string();

    let rhs_str = datafusion::arrow::util::pretty::pretty_format_batches(&rhs_batches)
        .unwrap()
        .to_string();

    pretty_assertions::assert_eq!(lhs_str, rhs_str);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn qualified_fields(
    schema: &DFSchema,
) -> impl Iterator<Item = (Option<&TableReference>, &Arc<Field>)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (schema.qualified_field(i).0, f))
}

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
