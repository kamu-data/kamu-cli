// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{Column, DFSchema};
use datafusion::prelude::col;
use datafusion::sql::TableReference;
use pretty_assertions::assert_eq;

use crate::data::DataFrameExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn assert_dfs_equal(lhs: DataFrameExt, rhs: DataFrameExt) {
    pretty_assertions::assert_eq!(lhs.schema(), rhs.schema());

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(RecordBatch::num_rows).sum();
    let rhs_count: usize = rhs_batches.iter().map(RecordBatch::num_rows).sum();

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Checks for equivalence ignoring the order of rows and nullability of columns
pub async fn assert_dfs_equivalent(
    lhs: DataFrameExt,
    rhs: DataFrameExt,
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
            .map(|f| col(Column::from_name(f.name())).sort(false, true))
            .collect();

        let rhs_cols = rhs
            .schema()
            .fields()
            .iter()
            .map(|f| col(Column::from_name(f.name())).sort(false, true))
            .collect();

        (lhs.sort(lhs_cols).unwrap(), rhs.sort(rhs_cols).unwrap())
    };

    let map_field =
        |(q, f): (Option<&TableReference>, &Arc<Field>)| -> (Option<TableReference>, Arc<Field>) {
            let q = q.cloned();
            let f = f.clone();
            let (q, f) = if !ignore_qualifiers {
                (q, f)
            } else {
                (None, f)
            };
            let f = if !ignore_nullability {
                f
            } else {
                Arc::new(f.as_ref().clone().with_nullable(true))
            };
            (q, f)
        };

    let lhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        qualified_fields(lhs.schema()).map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    let rhs_schema_stripped = datafusion::common::DFSchema::new_with_metadata(
        qualified_fields(rhs.schema()).map(map_field).collect(),
        Default::default(),
    )
    .unwrap();

    pretty_assertions::assert_eq!(lhs_schema_stripped, rhs_schema_stripped);

    let lhs_batches = lhs.collect().await.unwrap();
    let rhs_batches = rhs.collect().await.unwrap();

    let lhs_count: usize = lhs_batches.iter().map(RecordBatch::num_rows).sum();
    let rhs_count: usize = rhs_batches.iter().map(RecordBatch::num_rows).sum();

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn qualified_fields(
    schema: &DFSchema,
) -> impl Iterator<Item = (Option<&TableReference>, &Arc<Field>)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| (schema.qualified_field(i).0, f))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[expect(clippy::needless_pass_by_value)]
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
