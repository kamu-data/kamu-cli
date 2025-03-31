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
use odf::utils::data::DataFrameExt;

use crate::utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Op = odf::metadata::OperationType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
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

    for (y, c, p) in rows {
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

    ctx.read_batch(batch).unwrap().into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_output<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        // TODO: Replace with UInt8 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        Field::new("op", DataType::Int32, false),
        Field::new("year", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut op = Vec::new();
    let mut year = Vec::new();
    let mut city = Vec::new();
    let mut population = Vec::new();

    for (o, y, c, p) in rows {
        op.push(o as i32);
        year.push(y);
        city.push(c.into());
        population.push(p);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::Int32Array::from(op)),
            Arc::new(array::Int32Array::from(year)),
            Arc::new(array::StringArray::from(city)),
            Arc::new(array::Int64Array::from(population)),
        ],
    )
    .unwrap();

    ctx.read_batch(batch).unwrap().into()
}

fn make_output_empty(ctx: &SessionContext) -> DataFrameExt {
    make_output::<[_; 0], &str>(ctx, [])
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_ledger_merge<L, I, E, S>(ledger: L, input: I, expected: E)
where
    L: IntoIterator<Item = (Op, i32, S, i64)>,
    I: IntoIterator<Item = (i32, S, i64)>,
    E: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary {
            event_time_column: "year".to_string(),
            ..Default::default()
        },
        odf::metadata::MergeStrategyLedger {
            primary_key: vec!["year".to_string(), "city".to_string()],
        },
    );

    let prev = {
        let df = make_output(&ctx, ledger);
        if df.clone().count().await.unwrap() == 0 {
            None
        } else {
            Some(df)
        }
    };
    let new = make_input(&ctx, input);
    let expected = make_output(&ctx, expected);
    let actual = strat.merge(prev, new).unwrap();

    // Sort events according to the strategy
    let actual = actual.sort(strat.sort_order()).unwrap();

    assert_dfs_equivalent(expected, actual, false, true, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_to_empty() {
    test_ledger_merge(
        [],
        [
            (2020, "vancouver", 1),
            (2020, "seattle", 2),
            (2020, "kyiv", 3),
        ],
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_unseen() {
    test_ledger_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [(2020, "odessa", 4)],
        [(Op::Append, 2020, "odessa", 4)],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_seen_tail_ordered() {
    test_ledger_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [(2020, "seattle", 2), (2020, "kyiv", 3)],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_seen_tail_unordered() {
    test_ledger_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [(2020, "kyiv", 3), (2020, "seattle", 2)],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_seen_middle() {
    test_ledger_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [(2020, "seattle", 2)],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_respects_pk() {
    let ctx = SessionContext::new();

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "kiev", 3)]);
    let actual = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategyLedger {
            primary_key: vec!["year".to_string()],
        },
    )
    .merge(Some(prev), new)
    .unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual, false, true, true).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategyLedger {
            primary_key: vec!["year".to_string(), "city".to_string()],
        },
    )
    .merge(Some(prev), new)
    .unwrap();
    let expected = make_output_empty(&ctx);
    assert_dfs_equivalent(expected, actual, false, true, true).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2020, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategyLedger {
            primary_key: vec![
                "year".to_string(),
                "city".to_string(),
                "population".to_string(),
            ],
        },
    )
    .merge(Some(prev), new)
    .unwrap();
    let expected = make_output(&ctx, [(Op::Append, 2020, "seattle", 3)]);
    assert_dfs_equivalent(expected, actual, false, true, true).await;

    let prev = make_input(&ctx, [(2020, "vancouver", 1), (2020, "seattle", 2)]);
    let new = make_input(&ctx, [(2021, "seattle", 3)]);
    let actual = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategyLedger {
            primary_key: vec!["year".to_string(), "city".to_string()],
        },
    )
    .merge(Some(prev), new)
    .unwrap();
    let expected = make_output(&ctx, [(Op::Append, 2021, "seattle", 3)]);
    assert_dfs_equivalent(expected, actual, false, true, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_ledger_merge_invalid_pk() {
    let ctx = SessionContext::new();
    let strat = MergeStrategyLedger::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategyLedger {
            primary_key: vec!["year".to_string(), "city".to_string(), "foo".to_string()],
        },
    );

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
