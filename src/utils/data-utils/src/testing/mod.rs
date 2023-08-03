// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::common::DFSchema;
use datafusion::prelude::*;
use pretty_assertions::assert_eq;

pub fn assert_schema_eq(schema: &DFSchema, expected: &str) {
    let parquet_schema = crate::schema::convert::dataframe_schema_to_parquet_schema(schema);
    let actual = crate::schema::format::format_schema_parquet(&parquet_schema);
    assert_eq!(expected.trim(), actual.trim())
}

pub async fn assert_data_eq(df: DataFrame, expected: &str) {
    use datafusion::arrow::util::pretty;

    let batches = df.collect().await.unwrap();
    let actual = pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert_eq!(expected.trim(), actual.trim());
}
