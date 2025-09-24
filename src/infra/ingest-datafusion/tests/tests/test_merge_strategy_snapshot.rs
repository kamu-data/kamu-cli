// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::convert::TryFrom;
use std::sync::Arc;

use datafusion::arrow::array;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use kamu_ingest_datafusion::*;
use odf::utils::data::DataFrameExt;
use odf::utils::testing;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Op = odf::metadata::OperationType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("year", DataType::Int32, true),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut year = Vec::new();
    let mut city = Vec::new();
    let mut population = Vec::new();

    for (c, p) in rows {
        year.push(None);
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
    I: IntoIterator<Item = (odf::metadata::OperationType, Option<i32>, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        // TODO: Replace with UInt8 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        Field::new("op", DataType::Int32, false),
        Field::new("year", DataType::Int32, true),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_ledger<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (odf::metadata::OperationType, i32, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        // TODO: Replace with UInt64 and UInt8 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
        Field::new("offset", DataType::Int64, false),
        Field::new("op", DataType::Int32, false),
        Field::new("year", DataType::Int32, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("population", DataType::Int64, false),
    ]));

    let mut offset = Vec::new();
    let mut op = Vec::new();
    let mut year = Vec::new();
    let mut city = Vec::new();
    let mut population = Vec::new();

    for (i, (o, y, c, p)) in rows.into_iter().enumerate() {
        offset.push(i64::try_from(i).unwrap());
        op.push(o as i32);
        year.push(y);
        city.push(c.into());
        population.push(p);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(array::Int64Array::from(offset)),
            Arc::new(array::Int32Array::from(op)),
            Arc::new(array::Int32Array::from(year)),
            Arc::new(array::StringArray::from(city)),
            Arc::new(array::Int64Array::from(population)),
        ],
    )
    .unwrap();

    ctx.read_batch(batch).unwrap().into()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_snapshot_project<L, E, S>(ledger: L, expected: E)
where
    L: IntoIterator<Item = (odf::metadata::OperationType, i32, S, i64)>,
    E: IntoIterator<Item = (odf::metadata::OperationType, i32, S, i64)>,
    S: Into<String>,
{
    let ctx = SessionContext::new();
    let strat = MergeStrategySnapshot::new(
        odf::metadata::DatasetVocabulary {
            event_time_column: "year".to_string(),
            ..Default::default()
        },
        odf::metadata::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
        },
    );

    let ledger = make_ledger(&ctx, ledger);
    let expected = make_ledger(&ctx, expected)
        .without_columns(&["offset"])
        .unwrap();
    let actual = strat
        .project(ledger)
        .unwrap()
        .without_columns(&["offset"])
        .unwrap();

    testing::assert_dfs_equivalent(expected, actual, true, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot::project
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_project_empty() {
    test_snapshot_project::<_, _, &str>([], []).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_project_append() {
    test_snapshot_project(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
        ],
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_project_append_retract() {
    test_snapshot_project(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Retract, 2020, "vancouver", 0),
        ],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_project_correct() {
    test_snapshot_project(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 2),
        ],
        [(Op::CorrectTo, 2020, "vancouver", 2)],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_project_mixed() {
    test_snapshot_project(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 3),
            (Op::Append, 2020, "kyiv", 4),
            (Op::Retract, 2020, "seattle", 0),
            (Op::CorrectFrom, 2020, "kyiv", 4),
            (Op::CorrectTo, 2020, "kyiv", 1),
        ],
        [
            (Op::CorrectTo, 2020, "vancouver", 3),
            (Op::CorrectTo, 2020, "kyiv", 1),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MergeStrategySnapshot::merge
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_snapshot_merge<L, I, E, S>(ledger: L, input: I, expected: E)
where
    L: IntoIterator<Item = (odf::metadata::OperationType, i32, S, i64)>,
    I: IntoIterator<Item = (S, i64)>,
    E: IntoIterator<Item = (odf::metadata::OperationType, Option<i32>, S, i64)>,
    S: Into<String>,
{
    let ctx = SessionContext::new();

    let strat = MergeStrategySnapshot::new(
        odf::metadata::DatasetVocabulary {
            event_time_column: "year".to_string(),
            ..Default::default()
        },
        odf::metadata::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
        },
    );

    let prev = {
        let df = make_ledger(&ctx, ledger);
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

    testing::assert_dfs_equivalent(expected, actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_to_empty() {
    test_snapshot_merge(
        [],
        [("vancouver", 1), ("seattle", 2)],
        [
            (Op::Append, None, "seattle", 2),
            (Op::Append, None, "vancouver", 1),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_no_changes_ordered() {
    test_snapshot_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
        ],
        [("vancouver", 1), ("seattle", 2)],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_no_changes_unordered() {
    test_snapshot_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
        ],
        [("seattle", 2), ("vancouver", 1)],
        [],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_mix_of_changes() {
    test_snapshot_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [("seattle", 2), ("kyiv", 4), ("odessa", 5)],
        [
            (Op::CorrectFrom, Some(2020), "kyiv", 3),
            (Op::CorrectTo, None, "kyiv", 4),
            (Op::Append, None, "odessa", 5),
            (Op::Retract, Some(2020), "vancouver", 1),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_invalid_pk() {
    let ctx = SessionContext::new();
    let strat = MergeStrategySnapshot::new(
        odf::metadata::DatasetVocabulary::default(),
        odf::metadata::MergeStrategySnapshot {
            primary_key: vec!["city".to_string(), "foo".to_string()],
            compare_columns: None,
        },
    );

    let new = make_input(&ctx, [("vancouver", 1), ("seattle", 2)]);

    let res = strat.merge(None, new);
    assert_matches!(res, Err(MergeError::Internal(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_snapshot_merge_input_carries_event_time() {
    let ctx = SessionContext::new();

    let strat = MergeStrategySnapshot::new(
        odf::metadata::DatasetVocabulary {
            event_time_column: "year".to_string(),
            ..Default::default()
        },
        odf::metadata::MergeStrategySnapshot {
            primary_key: vec!["city".to_string()],
            compare_columns: None,
        },
    );

    let prev = make_ledger(
        &ctx,
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
    );

    let new = make_output(
        &ctx,
        [
            (Op::Append, Some(2020), "seattle", 2),
            (Op::Append, Some(2021), "kyiv", 3),
            (Op::Append, Some(2021), "odessa", 5),
        ],
    )
    .without_columns(&["op"])
    .unwrap();

    let expected = make_output(
        &ctx,
        [
            (Op::CorrectFrom, Some(2020), "kyiv", 3),
            (Op::CorrectTo, Some(2021), "kyiv", 3),
            (Op::Append, Some(2021), "odessa", 5),
            (Op::Retract, Some(2020), "vancouver", 1),
        ],
    );

    let actual = strat.merge(Some(prev), new).unwrap();

    // Sort events according to the strategy
    let actual = actual.sort(strat.sort_order()).unwrap();

    testing::assert_dfs_equivalent(expected, actual, false, false, true).await;
}
