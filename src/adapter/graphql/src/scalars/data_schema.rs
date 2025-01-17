// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use odf::utils::schema::{convert, format};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSchemaFormat {
    Parquet,
    ParquetJson,
    ArrowJson,
}

#[derive(SimpleObject, Debug, Clone, PartialEq, Eq)]
pub struct DataSchema {
    pub format: DataSchemaFormat,
    pub content: String,
}

impl DataSchema {
    pub fn from_arrow_schema(
        schema: &datafusion::arrow::datatypes::Schema,
        format: DataSchemaFormat,
    ) -> DataSchema {
        match format {
            DataSchemaFormat::ArrowJson => {
                let mut buf = Vec::new();
                format::write_schema_arrow_json(&mut buf, schema).unwrap();

                DataSchema {
                    format,
                    content: String::from_utf8(buf).unwrap(),
                }
            }
            DataSchemaFormat::Parquet | DataSchemaFormat::ParquetJson => {
                let parquet_schema = convert::arrow_schema_to_parquet_schema(schema);
                Self::from_parquet_schema(&parquet_schema, format)
            }
        }
    }

    pub fn from_parquet_schema(
        schema: &datafusion::parquet::schema::types::Type,
        format: DataSchemaFormat,
    ) -> DataSchema {
        let mut buf = Vec::new();

        match format {
            DataSchemaFormat::Parquet => {
                format::write_schema_parquet(&mut buf, schema).unwrap();
            }
            DataSchemaFormat::ParquetJson => {
                format::write_schema_parquet_json(&mut buf, schema).unwrap();
            }
            _ => unreachable!(),
        }
        DataSchema {
            format,
            content: String::from_utf8(buf).unwrap(),
        }
    }

    pub fn from_data_frame_schema(
        schema: &datafusion::common::DFSchema,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        let mut buf = Vec::new();
        match format {
            DataSchemaFormat::Parquet => format::write_schema_parquet(
                &mut buf,
                convert::dataframe_schema_to_parquet_schema(schema).as_ref(),
            ),
            DataSchemaFormat::ParquetJson => format::write_schema_parquet_json(
                &mut buf,
                convert::dataframe_schema_to_parquet_schema(schema).as_ref(),
            ),
            DataSchemaFormat::ArrowJson => {
                let arrow_schema = datafusion::arrow::datatypes::Schema::from(schema);
                format::write_schema_arrow_json(&mut buf, &arrow_schema)
            }
        }
        .int_err()?;

        Ok(DataSchema {
            format,
            content: String::from_utf8(buf).unwrap(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use datafusion::arrow::array::{Int32Array, RecordBatch, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::context::SessionContext;
    use serde_json::Value;

    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_data_schema_parquet() {
        let df = get_test_df().await;

        let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::Parquet);

        let data_schema = result.unwrap();

        assert_eq!(data_schema.format, DataSchemaFormat::Parquet);

        let expected_content = indoc::indoc!(
            r#"
            message arrow_schema {
              REQUIRED BYTE_ARRAY a (STRING);
              REQUIRED INT32 b;
            }
            "#
        );

        let actual_content = data_schema.content.as_str();

        assert_eq!(actual_content, expected_content);
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_parquet_json() {
        let df = get_test_df().await;

        let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::ParquetJson);

        let data_schema = result.unwrap();

        assert_eq!(data_schema.format, DataSchemaFormat::ParquetJson);

        let schema_content = data_schema.content;

        let data_schema_json = serde_json::from_str::<Value>(schema_content.as_str()).unwrap();

        assert_eq!(
            data_schema_json,
            serde_json::json!({
                "fields": [{
                    "logicalType": "STRING",
                    "name": "a",
                    "repetition": "REQUIRED",
                    "type": "BYTE_ARRAY"
                }, {
                    "name": "b",
                    "repetition": "REQUIRED",
                    "type": "INT32"
                }],
                "name": "arrow_schema",
                "type": "struct"
            })
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_parquet_json_escaping() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a \" b",
            DataType::Utf8,
            false,
        )]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(vec!["a"]))]).unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        let df = ctx.table("t").await.unwrap();

        let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::ParquetJson);

        let data_schema = result.unwrap();

        assert_eq!(data_schema.format, DataSchemaFormat::ParquetJson);

        let schema_content = data_schema.content;

        let data_schema_json = serde_json::from_str::<Value>(schema_content.as_str()).unwrap();

        assert_eq!(
            data_schema_json,
            serde_json::json!({
                "fields": [{
                    "logicalType": "STRING",
                    "name": "a \" b",
                    "repetition": "REQUIRED",
                    "type": "BYTE_ARRAY"
                }],
                "name": "arrow_schema",
                "type": "struct"
            })
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_arrow_json() {
        let df = get_test_df().await;

        let result = DataSchema::from_data_frame_schema(df.schema(), DataSchemaFormat::ArrowJson);

        let data_schema = result.unwrap();
        let schema_content = data_schema.content;

        let data_schema_json =
            serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

        assert_eq!(
            data_schema_json,
            serde_json::json!({
                "fields": [{
                    "data_type": "Utf8",
                    "dict_id": 0,
                    "dict_is_ordered": false,
                    "metadata": {},
                    "name": "a",
                    "nullable": false
                }, {
                    "data_type": "Int32",
                    "dict_id": 0,
                    "dict_is_ordered": false,
                    "metadata": {},
                    "name": "b",
                    "nullable": false
                }],
                "metadata": {}
            })
        );
    }

    async fn get_test_df() -> DataFrame {
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
        )
        .unwrap();

        let ctx = SessionContext::new();
        ctx.register_batch("t", batch).unwrap();
        ctx.table("t").await.unwrap()
    }
}
