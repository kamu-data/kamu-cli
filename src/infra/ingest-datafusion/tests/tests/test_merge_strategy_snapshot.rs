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
use kamu_data_utils::data::dataframe_ext::*;
use kamu_ingest_datafusion::*;

use crate::utils::*;

///////////////////////////////////////////////////////////////////////////////

async fn make_input<I, S>(ctx: &SessionContext, table_name: &str, rows: I) -> DataFrame
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

    for (c, p) in rows.into_iter() {
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

    ctx.register_batch(table_name, batch).unwrap();
    ctx.table(table_name).await.unwrap()
}

///////////////////////////////////////////////////////////////////////////////

async fn make_output<I, C, S>(ctx: &SessionContext, table_name: &str, rows: I) -> DataFrame
where
    I: IntoIterator<Item = (C, S, i64)>,
    C: Into<String>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            opendatafabric::MergeStrategySnapshot::DEFAULT_OBSV_COLUMN_NAME,
            DataType::Utf8,
            false,
        ),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut obsv = Vec::new();
    let mut city = Vec::new();
    let mut population = Vec::new();

    for (o, c, p) in rows.into_iter() {
        obsv.push(o.into());
        city.push(c.into());
        population.push(p);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::StringArray::from(obsv)),
            Arc::new(array::StringArray::from(city)),
            Arc::new(array::Int64Array::from(population)),
        ],
    )
    .unwrap();

    ctx.register_batch(table_name, batch).unwrap();
    ctx.table(table_name).await.unwrap()
}

async fn make_output_empty(ctx: &SessionContext, table_name: &str) -> DataFrame {
    make_output::<[_; 0], &str, &str>(ctx, table_name, []).await
}

///////////////////////////////////////////////////////////////////////////////

async fn make_ledger<I, C, S>(ctx: &SessionContext, table_name: &str, rows: I) -> DataFrame
where
    I: IntoIterator<Item = (C, S, i64)>,
    C: Into<String>,
    S: Into<String>,
{
    with_offsets(make_output(ctx, table_name, rows).await)
}

///////////////////////////////////////////////////////////////////////////////

fn with_offsets(df: DataFrame) -> DataFrame {
    df.window(vec![Expr::WindowFunction(
        datafusion::logical_expr::expr::WindowFunction {
            fun: datafusion::logical_expr::WindowFunction::BuiltInWindowFunction(
                datafusion::logical_expr::BuiltInWindowFunction::RowNumber,
            ),
            args: Vec::new(),
            partition_by: vec![],
            order_by: vec![],
            window_frame: datafusion::logical_expr::WindowFrame::new(false),
        },
    )
    .alias("offset")])
        .unwrap()
}

///////////////////////////////////////////////////////////////////////////////

fn get_strat() -> MergeStrategySnapshot {
    MergeStrategySnapshot::new(
        "offset".to_string(),
        opendatafabric::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
            observation_column: None,
            obsv_added: None,
            obsv_changed: None,
            obsv_removed: None,
        },
    )
}

///////////////////////////////////////////////////////////////////////////////

async fn test_snapshot_project<I1, C1, S1, I2, S2>(ledger: I1, expected: I2)
where
    I1: IntoIterator<Item = (C1, S1, i64)>,
    C1: Into<String>,
    S1: Into<String>,
    I2: IntoIterator<Item = (S2, i64)>,
    S2: Into<String>,
{
    let ctx = SessionContext::new();
    let strat: MergeStrategySnapshot = get_strat();

    let ledger = make_ledger(&ctx, "ledger", ledger).await;
    let expected = make_input(&ctx, "expected", expected).await;

    let actual = strat.project(ledger).unwrap();
    let actual = actual.without_columns(&["offset", "observed"]).unwrap();

    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot::project
///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_project_empty() {
    test_snapshot_project::<[_; 0], &str, &str, [_; 0], &str>([], []).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_project_add() {
    test_snapshot_project(
        [("I", "vancouver", 1), ("I", "seattle", 2)],
        [("vancouver", 1), ("seattle", 2)],
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_project_add_remove() {
    test_snapshot_project::<_, _, _, [_; 0], &str>(
        [("I", "vancouver", 1), ("D", "vancouver", 0)],
        [],
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_project_update() {
    test_snapshot_project(
        [("I", "vancouver", 1), ("U", "vancouver", 2)],
        [("vancouver", 2)],
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_project_mixed() {
    test_snapshot_project(
        [
            ("I", "vancouver", 1),
            ("I", "seattle", 2),
            ("U", "vancouver", 3),
            ("I", "kyiv", 4),
            ("D", "seattle", 0),
            ("U", "kyiv", 1),
        ],
        [("vancouver", 3), ("kyiv", 1)],
    )
    .await;
}

///////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot::merge
///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_to_empty() {
    let ctx = SessionContext::new();
    let strat = get_strat();

    let prev = make_ledger::<[_; 0], &str, &str>(&ctx, "prev", []).await;
    let new = make_input(&ctx, "new", [("vancouver", 1), ("seattle", 2)]).await;

    let actual = strat.merge(prev, new).unwrap();
    let expected = make_output(
        &ctx,
        "expected",
        [("I", "vancouver", 1), ("I", "seattle", 2)],
    )
    .await;
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_no_changes_ordered() {
    let ctx = SessionContext::new();
    let strat = get_strat();

    let prev = make_ledger(&ctx, "prev", [("I", "vancouver", 1), ("I", "seattle", 2)]).await;
    let new = make_input(&ctx, "new", [("vancouver", 1), ("seattle", 2)]).await;

    let actual = strat.merge(prev, new).unwrap();
    let expected = make_output_empty(&ctx, "expected").await;
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_no_changes_unordered() {
    let ctx = SessionContext::new();
    let strat = get_strat();

    let prev = make_ledger(&ctx, "prev", [("I", "vancouver", 1), ("I", "seattle", 2)]).await;
    let new = make_input(&ctx, "new", [("seattle", 2), ("vancouver", 1)]).await;

    let actual = strat.merge(prev, new).unwrap();
    let expected = make_output_empty(&ctx, "expected").await;
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_snapshot_mix_of_changes() {
    let ctx = SessionContext::new();
    let strat = get_strat();

    let prev = make_ledger(
        &ctx,
        "prev",
        [("I", "vancouver", 1), ("I", "seattle", 2), ("I", "kyiv", 3)],
    )
    .await;
    let new = make_input(&ctx, "new", [("seattle", 2), ("kyiv", 4), ("odessa", 5)]).await;

    let actual = strat.merge(prev, new).unwrap();
    let expected = make_output(
        &ctx,
        "expected",
        [("D", "vancouver", 1), ("U", "kyiv", 4), ("I", "odessa", 5)],
    )
    .await;

    actual.clone().show().await.unwrap();

    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////
