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

use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use kamu_ingest_datafusion::*;

use crate::utils::*;

///////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrame
where
    I: IntoIterator<Item = (i32, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut year = Vec::new();
    let mut city = Vec::new();
    let mut population = Vec::new();

    for (y, c, p) in rows.into_iter() {
        year.push(y);
        city.push(c.into());
        population.push(p);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::Int32Array::from(year)),
            Arc::new(array::StringArray::from(city)),
            Arc::new(array::Int64Array::from(population)),
        ],
    )
    .unwrap();

    ctx.read_batch(batch).unwrap()
}

fn make_output_empty(ctx: &SessionContext) -> DataFrame {
    make_input::<[_; 0], &str>(&ctx, [])
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_to_empty() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()]);

    let new = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );

    let actual = strat.merge(None, new).unwrap();
    let expected = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_unseen() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()]);

    let prev = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );
    let new = make_input(&ctx, [(2020, "odessa", 4)]);

    let actual = strat.merge(Some(prev), new).unwrap();
    let expected = make_input(&ctx, [(2020, "odessa", 4)]);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_seen_tail_ordered() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()]);

    let prev = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );
    let new = make_input(&ctx, [(2020, "seattle", 2), (2020, "kyiv", 3)]);

    let actual = strat.merge(Some(prev), new).unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_seen_tail_unordered() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()]);

    let prev = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );
    let new = make_input(&ctx, [(2020, "kyiv", 3), (2020, "seattle", 2)]);

    let actual = strat.merge(Some(prev), new).unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_seen_middle() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()]);

    let prev = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );
    let new = make_input(&ctx, [(2020, "seattle", 2)]);

    let actual = strat.merge(Some(prev), new).unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_respects_pk() {
    let ctx = SessionContext::new();

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "kiev", 3)]);
    let actual = MergeStrategyLedger::new(vec!["year".to_string()])
        .merge(Some(prev), new)
        .unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()])
        .merge(Some(prev), new)
        .unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(vec![
        "year".to_string(),
        "city".to_string(),
        "population".to_string(),
    ])
    .merge(Some(prev), new)
    .unwrap();
    let expected = make_input(&ctx, [(2020, "seattle", 3)]);
    assert_dfs_equivalent(expected, actual).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2021, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(vec!["year".to_string(), "city".to_string()])
        .merge(Some(prev), new)
        .unwrap();
    let expected = make_input(&ctx, [(2021, "seattle", 3)]);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_ledger_invalid_pk() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(vec![
        "year".to_string(),
        "city".to_string(),
        "foo".to_string(),
    ]);

    let new = make_input(
        &ctx,
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
    );

    let res = strat.merge(None, new);
    assert_matches!(res, Err(MergeError::Internal(_)));
}

///////////////////////////////////////////////////////////////////////////////
