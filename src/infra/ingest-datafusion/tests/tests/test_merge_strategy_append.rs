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

    ctx.read_batch(batch).unwrap()
}

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_append_to_empty() {
    let ctx = SessionContext::new();
    let new = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);

    let actual = MergeStrategyAppend.merge(None, new).unwrap();
    let expected = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, ingest, datafusion)]
#[test_log::test(tokio::test)]
async fn test_append_to_some() {
    let ctx = SessionContext::new();
    let prev = make_input(&ctx, [("vancouver", 1), ("seattle", 2), ("kyiv", 3)]);
    let new = make_input(&ctx, [("vancouver", 1), ("odessa", 4)]);

    let actual = MergeStrategyAppend.merge(Some(prev), new).unwrap();
    let expected = make_input(&ctx, [("vancouver", 1), ("odessa", 4)]);
    assert_dfs_equivalent(expected, actual).await;
}

///////////////////////////////////////////////////////////////////////////////
