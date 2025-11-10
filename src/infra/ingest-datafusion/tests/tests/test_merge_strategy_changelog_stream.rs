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
use odf::utils::data::dataframe_ext::DataFrameExt;
use odf::utils::testing;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Op = odf::metadata::OperationType;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_input<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(clippy::cast_possible_wrap)]
fn make_output<I, S>(ctx: &SessionContext, rows: I) -> DataFrameExt
where
    I: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::Int64, false),
        // TODO: Replace with UInt8 after Spark is updated
        // See: https://github.com/kamu-data/kamu-cli/issues/445
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

    for (off, (o, y, c, p)) in rows.into_iter().enumerate() {
        offset.push(off as i64);
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

async fn test_changelog_merge<L, I, E, S>(ledger: L, input: I, expected: E)
where
    L: IntoIterator<Item = (Op, i32, S, i64)>,
    I: IntoIterator<Item = (Op, i32, S, i64)>,
    E: IntoIterator<Item = (Op, i32, S, i64)>,
    S: Into<String>,
{
    let ctx = SessionContext::new();
    let strat = MergeStrategyChangelogStream::new(
        odf::metadata::DatasetVocabulary {
            event_time_column: "year".to_string(),
            ..Default::default()
        },
        odf::metadata::MergeStrategyChangelogStream {
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
    let expected = make_output(&ctx, expected)
        .without_columns(&["offset"])
        .unwrap();
    let actual = strat.merge(prev, new).unwrap();

    // Sort events according to the strategy
    let actual = actual.sort(strat.sort_order()).unwrap();

    testing::assert_dfs_equivalent(expected, actual, false, false, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_changelog_merge_to_empty() {
    test_changelog_merge(
        [],
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 2),
        ],
        [
            (Op::Append, 2020, "kyiv", 3),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "vancouver", 1),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 2),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_changelog_merge_unseen() {
    test_changelog_merge(
        [
            (Op::Append, 2020, "vancouver", 1),
            (Op::Append, 2020, "seattle", 2),
            (Op::Append, 2020, "kyiv", 3),
        ],
        [
            (Op::Append, 2020, "odessa", 4),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 2),
        ],
        [
            (Op::Append, 2020, "odessa", 4),
            (Op::CorrectFrom, 2020, "vancouver", 1),
            (Op::CorrectTo, 2020, "vancouver", 2),
        ],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
