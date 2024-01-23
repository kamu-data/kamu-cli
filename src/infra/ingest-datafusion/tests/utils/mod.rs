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
use datafusion::common::DFField;
use datafusion::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub async fn assert_dfs_equal(lhs: DataFrame, rhs: DataFrame) {
    pretty_assertions::assert_eq!(lhs.schema(), rhs.schema());

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(|b| b.num_rows()).sum();
    let rhs_count: usize = rhs_batches.iter().map(|b| b.num_rows()).sum();

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

///////////////////////////////////////////////////////////////////////////////

// Checks for equivalence ignoring the order of rows and nullability of columns
pub async fn assert_dfs_equivalent(
    lhs: DataFrame,
    rhs: DataFrame,
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
            .map(|f| col(f.name()).sort(false, true))
            .collect();

        let rhs_cols = rhs
            .schema()
            .fields()
            .iter()
            .map(|f| col(f.name()).sort(false, true))
            .collect();

        (lhs.sort(lhs_cols).unwrap(), rhs.sort(rhs_cols).unwrap())
    };

    let map_field = |f: &DFField| -> DFField {
        let f = f.clone();
        let f = if !ignore_qualifiers {
            f
        } else {
            f.strip_qualifier()
        };
        if !ignore_nullability {
            f
        } else {
            f.with_nullable(true)
        }
    };

    let lhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        lhs.schema().fields().iter().map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    let rhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        rhs.schema().fields().iter().map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(lhs_schema_stripped, rhs_schema_stripped);

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(|b| b.num_rows()).sum();
    let rhs_count: usize = rhs_batches.iter().map(|b| b.num_rows()).sum();

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

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_assert_dfs_equal_success() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

#[test_log::test(tokio::test)]
#[should_panic]
async fn test_assert_dfs_equal_fails_order() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("seattle", 2), ("vancouver", 1), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

#[test_log::test(tokio::test)]
#[should_panic]
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
#[should_panic]
async fn test_assert_dfs_equal_fails_column() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("vancouver", 1), ("seattle", 1), ("kyiv", 3)]);
    assert_dfs_equal(a, b).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_assert_dfs_equivalent_success() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("kyiv", 3), ("vancouver", 1), ("seattle", 2)]);
    assert_dfs_equivalent(a, b, true, true, true).await;
}

#[test_log::test(tokio::test)]
#[should_panic]
async fn test_assert_dfs_equivalent_fails_column() {
    let ctx = SessionContext::new();

    let a = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let b = make_input(&ctx, [("kyiv", 3), ("vancouver", 2), ("seattle", 2)]);
    assert_dfs_equivalent(a, b, true, true, true).await;
}

///////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrame
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

    ctx.read_batch(batch).unwrap()
}
