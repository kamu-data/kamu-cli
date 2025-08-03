// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use pretty_assertions::assert_eq;

use crate::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::needless_pass_by_value)]
pub fn assert_odf_schema_eq(
    actual: &odf_metadata::DataSchema,
    expected: &odf_metadata::DataSchema,
) {
    let actual_yaml = crate::schema::format::format_schema_odf_yaml(actual);
    let expected_yaml = crate::schema::format::format_schema_odf_yaml(expected);
    assert_eq!(expected_yaml, actual_yaml);
}

pub fn assert_schema_eq(schema: &DFSchema, expected: &str) {
    let parquet_schema = crate::schema::convert::dataframe_schema_to_parquet_schema(schema);
    let actual = crate::schema::format::format_schema_parquet(&parquet_schema);
    assert_eq!(expected.trim(), actual.trim());
}

#[allow(clippy::needless_pass_by_value)]
pub fn assert_arrow_schema_eq(schema: &Schema, expected: serde_json::Value) {
    let actual = serde_json::to_value(schema).unwrap();
    assert_eq!(expected, actual);
}

pub async fn assert_data_eq(df: DataFrameExt, expected: &str) {
    use datafusion::arrow::util::pretty;

    let batches = df.collect().await.unwrap();
    let actual = pretty::pretty_format_batches(&batches).unwrap().to_string();
    assert_eq!(expected.trim(), actual.trim());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn assert_parquet_offsets_are_in_order(data_path: &std::path::Path) {
    use ::datafusion::arrow::array::{Int64Array, downcast_array};
    use ::datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let reader = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(data_path).unwrap())
        .unwrap()
        .build()
        .unwrap();

    let mut expected_offset = 0;

    for batch in reader {
        let batch = batch.unwrap();
        let offsets_dyn = batch.column_by_name("offset").unwrap();
        let offsets = downcast_array::<Int64Array>(offsets_dyn);
        for i in 0..offsets.len() {
            let actual_offset = offsets.value(i);
            assert_eq!(
                actual_offset, expected_offset,
                "Offset column in parquet file is not sequentially ordered"
            );
            expected_offset += 1;
        }
    }
}
