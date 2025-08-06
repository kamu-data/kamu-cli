// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use odf::utils::schema::{convert, format};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSchemaFormat {
    ArrowJson,
    OdfJson,
    OdfYaml,
    Parquet,
    ParquetJson,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataSchema {
    pub schema: Arc<odf::metadata::DataSchema>,
    pub format: DataSchemaFormat,
}

impl DataSchema {
    pub fn new(schema: Arc<odf::metadata::DataSchema>, format: DataSchemaFormat) -> Self {
        Self { schema, format }
    }

    pub fn new_empty(format: DataSchemaFormat) -> Self {
        Self {
            schema: Arc::new(odf::metadata::DataSchema::new_empty()),
            format,
        }
    }

    pub fn new_from_arrow(
        schema: &datafusion::arrow::datatypes::Schema,
        format: DataSchemaFormat,
    ) -> Self {
        Self {
            schema: Arc::new(odf::metadata::DataSchema::new_from_arrow(schema).strip_encoding()),
            format,
        }
    }

    pub fn new_from_data_frame_schema(
        schema: &datafusion::common::DFSchema,
        format: DataSchemaFormat,
    ) -> Self {
        Self::new_from_arrow(schema.inner(), format)
    }

    pub(crate) fn content_impl(&self) -> Result<String> {
        let mut buf = Vec::new();

        match self.format {
            DataSchemaFormat::ArrowJson => {
                format::write_schema_arrow_json(&mut buf, &self.schema.to_arrow()).int_err()?;
            }
            DataSchemaFormat::OdfJson => {
                let mut serializer = serde_json::Serializer::new(&mut buf);
                odf::serde::yaml::DataSchemaDef::serialize(&self.schema, &mut serializer)
                    .int_err()?;
            }
            DataSchemaFormat::OdfYaml => {
                let mut serializer = serde_yaml::Serializer::new(&mut buf);
                odf::serde::yaml::DataSchemaDef::serialize(&self.schema, &mut serializer)
                    .int_err()?;
            }
            DataSchemaFormat::Parquet => {
                format::write_schema_parquet(
                    &mut buf,
                    convert::arrow_schema_to_parquet_schema(&self.schema.to_arrow()).as_ref(),
                )
                .int_err()?;
            }
            DataSchemaFormat::ParquetJson => {
                format::write_schema_parquet_json(
                    &mut buf,
                    convert::arrow_schema_to_parquet_schema(&self.schema.to_arrow()).as_ref(),
                )
                .int_err()?;
            }
        }

        Ok(String::from_utf8(buf).unwrap())
    }
}

#[Object]
impl DataSchema {
    pub async fn format(&self) -> DataSchemaFormat {
        self.format
    }

    #[expect(clippy::unused_async)]
    pub async fn content(&self) -> Result<String> {
        self.content_impl()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Input types
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Defines a dataset column
#[derive(InputObject)]
pub struct ColumnInput {
    /// Column name
    pub name: String,

    /// Column data type
    #[graphql(name = "type")]
    pub data_type: DataTypeInput,
}

#[derive(InputObject)]
pub struct DataTypeInput {
    /// Defines type using DDL syntax
    pub ddl: String,
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

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::Parquet);

        assert_eq!(data_schema.format, DataSchemaFormat::Parquet);

        let expected_content = indoc::indoc!(
            r#"
            message arrow_schema {
              REQUIRED BYTE_ARRAY a (STRING);
              REQUIRED INT32 b;
            }
            "#
        );

        let actual_content = data_schema.content_impl().unwrap();

        pretty_assertions::assert_eq!(actual_content, expected_content);
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_parquet_json() {
        let df = get_test_df().await;

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::ParquetJson);

        assert_eq!(data_schema.format, DataSchemaFormat::ParquetJson);

        let schema_content = data_schema.content_impl().unwrap();

        let data_schema_json = serde_json::from_str::<Value>(schema_content.as_str()).unwrap();

        pretty_assertions::assert_eq!(
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

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::ParquetJson);

        assert_eq!(data_schema.format, DataSchemaFormat::ParquetJson);

        let schema_content = data_schema.content_impl().unwrap();

        let data_schema_json = serde_json::from_str::<Value>(schema_content.as_str()).unwrap();

        pretty_assertions::assert_eq!(
            serde_json::json!({
                "fields": [{
                    "logicalType": "STRING",
                    "name": "a \" b",
                    "repetition": "REQUIRED",
                    "type": "BYTE_ARRAY"
                }],
                "name": "arrow_schema",
                "type": "struct"
            }),
            data_schema_json,
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_arrow_json() {
        let df = get_test_df().await;

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::ArrowJson);

        let schema_content = data_schema.content_impl().unwrap();

        let data_schema_json =
            serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

        pretty_assertions::assert_eq!(
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
            }),
            data_schema_json,
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_odf_json() {
        let df = get_test_df().await;

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::OdfJson);

        let schema_content = data_schema.content_impl().unwrap();

        let data_schema_json =
            serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

        pretty_assertions::assert_eq!(
            serde_json::json!({
                "fields": [{
                    "name": "a",
                    "type": {
                        "kind": "String",
                    },
                }, {
                    "name": "b",
                    "type": {
                        "bitWidth": 32,
                        "kind": "Int",
                        "signed": true,
                    },
                }]
            }),
            data_schema_json,
        );
    }

    #[test_log::test(tokio::test)]
    async fn test_data_schema_odf_yaml() {
        let df = get_test_df().await;

        let data_schema =
            DataSchema::new_from_data_frame_schema(df.schema(), DataSchemaFormat::OdfYaml);

        assert_eq!(data_schema.format, DataSchemaFormat::OdfYaml);

        let expected_content = indoc::indoc!(
            r#"
            fields:
            - name: a
              type:
                kind: String
            - name: b
              type:
                kind: Int
                bitWidth: 32
                signed: true
            "#
        );

        let actual_content = data_schema.content_impl().unwrap();

        pretty_assertions::assert_eq!(actual_content, expected_content);
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
