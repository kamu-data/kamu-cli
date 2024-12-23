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
use kamu_ingest_datafusion::*;

use crate::utils::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Op = odf::metadata::OperationType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

    ctx.read_batch(batch).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_output<I, S>(ctx: &SessionContext, rows: I) -> DataFrame
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

    ctx.read_batch(batch).unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_append_merge<L, I, E, S>(ledger: L, input: I, expected: E)
where
    L: IntoIterator<Item = (Op, i32, S, i64)>,
    I: IntoIterator<Item = (i32, S, i64)>,
    E: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let ctx = SessionContext::new();
    let strat = MergeStrategyAppend::new(odf::metadata::DatasetVocabulary {
        event_time_column: "year".to_string(),
        ..Default::default()
    });
    let prev = {
        let df = make_output(&ctx, ledger);
        if df.clone().count().await.unwrap() == 0 {
            None
        } else {
            Some(df)
        }
    };
    let new = make_input(&ctx, input);
    let actual = strat.merge(prev, new).unwrap();
    let expected = make_output(&ctx, expected);

    // Sort events according to the strategy
    let actual = actual.sort(strat.sort_order()).unwrap();

    assert_dfs_equivalent(expected, actual, false, true, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_append_merge_to_empty() {
    test_append_merge(
        [],
        [
            (2020, "vancouver", 1),
            (2021, "seattle", 2),
            (2022, "kyiv", 3),
        ],
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2021, "seattle", 2),
            (Op::Append, 2022, "kyiv", 3),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_append_merge_to_some() {
    test_append_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2021, "seattle", 2),
            (Op::Append, 2022, "kyiv", 3),
        ],
        [(2023, "vancouver", 1), (2024, "odessa", 4)],
        [
            (Op::Append, 2023, "vancouver", 1),
            (Op::Append, 2024, "odessa", 4),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
