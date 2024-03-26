// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::prelude::*;
use kamu_adapter_graphql::scalars::data_schema::{DataSchema, DataSchemaFormat};
use serde_json::{to_string_pretty, Value};

#[test_log::test(tokio::test)]
async fn test_from_parquet_schema_parquet() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    let filter = col("b").eq(lit(10));

    let df = df.select_columns(&["a", "b"])?.filter(filter)?;

    let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::Parquet);

    assert!(result.is_ok());
    let data_schema = result.unwrap();

    let expected_content = indoc::indoc!(
        r#"
    message arrow_schema {
      REQUIRED BYTE_ARRAY a (STRING);
      REQUIRED INT32 b;
    }
    "#
    );

    let actual_content = data_schema.content.as_str();

    assert_eq!(data_schema.format, DataSchemaFormat::Parquet);
    assert_eq!(actual_content, expected_content);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_from_parquet_schema_parquet_json() -> Result<()> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    let filter = col("b").eq(lit(10));

    let df = df.select_columns(&["a", "b"])?.filter(filter)?;

    let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::ParquetJson);

    assert!(result.is_ok());
    let data_schema = result.unwrap();

    let expected_content = indoc::indoc!(
        r#"{
        "fields": [
          {
            "logicalType": "STRING",
            "name": "a",
            "repetition": "REQUIRED",
            "type": "BYTE_ARRAY"
          },
          {
            "name": "b",
            "repetition": "REQUIRED",
            "type": "INT32"
          }
        ],
        "name": "arrow_schema",
        "type": "struct"
      }"#
    );

    assert_eq!(data_schema.format, DataSchemaFormat::ParquetJson);

    let actual_content = beautify_json(data_schema.content.as_str()).unwrap();

    assert_eq!(actual_content, expected_content);

    Ok(())
}

#[test_log::test(tokio::test)]
async fn test_from_parquet_schema_parquet_arrow_json() -> Result<()> {
    let df = get_test_df_schema().await?;

    let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::ArrowJson);

    assert!(result.is_ok());
    let data_schema = result.unwrap();

    println!("{}", data_schema.content.as_str());

    assert_eq!(data_schema.format, DataSchemaFormat::ArrowJson);
    let expected_content = indoc::indoc!(
        r#"{
     "fields": [
       {
         "data_type": "Utf8",
         "dict_id": 0,
         "dict_is_ordered": false,
         "metadata": {},
         "name": "a",
         "nullable": false
       },
       {
         "data_type": "Int32",
         "dict_id": 0,
         "dict_is_ordered": false,
         "metadata": {},
         "name": "b",
         "nullable": false
       }
     ],
     "metadata": {}
   }"#
    );

    let actual_content = beautify_json(data_schema.content.as_str()).unwrap();

    assert_eq!(actual_content, expected_content);

    Ok(())
}

fn beautify_json(json_str: &str) -> Result<String, serde_json::Error> {
    let value: Value = serde_json::from_str(json_str)?;
    to_string_pretty(&value)
}

async fn get_test_df_schema() -> Result<DataFrame, DataFusionError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int32, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            Arc::new(Int32Array::from(vec![1, 10, 10, 100])),
        ],
    )?;

    let ctx = SessionContext::new();

    ctx.register_batch("t", batch)?;
    let df = ctx.table("t").await?;

    let filter = col("b").eq(lit(10));

    let df = df.select_columns(&["a", "b"])?.filter(filter)?;
    Ok(df)
}
