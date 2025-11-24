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
    pub schema: Arc<odf::schema::DataSchema>,
    pub format: DataSchemaFormat,
}

impl DataSchema {
    pub fn new(schema: Arc<odf::schema::DataSchema>, format: DataSchemaFormat) -> Self {
        Self { schema, format }
    }

    pub fn new_empty(format: DataSchemaFormat) -> Self {
        Self {
            schema: Arc::new(odf::schema::DataSchema::new_empty()),
            format,
        }
    }

    pub fn new_from_arrow_stripped(
        schema: &datafusion::arrow::datatypes::Schema,
        format: DataSchemaFormat,
    ) -> Result<Self> {
        // NOTE: We strip encoding as we are only interested in logical types
        let schema_odf = odf::schema::DataSchema::new_from_arrow(schema)
            .int_err()?
            .strip_encoding();
        Ok(Self {
            schema: Arc::new(schema_odf),
            format,
        })
    }

    pub fn new_from_data_frame_schema_stripped(
        schema: &datafusion::common::DFSchema,
        format: DataSchemaFormat,
    ) -> Result<Self> {
        Self::new_from_arrow_stripped(schema.inner(), format)
    }

    pub(crate) fn content_impl(&self) -> Result<String> {
        match self.format {
            DataSchemaFormat::ArrowJson => Ok(format::format_schema_arrow_json(
                &self
                    .schema
                    .to_arrow(&odf::metadata::ToArrowSettings::default())
                    .int_err()?,
            )),
            DataSchemaFormat::OdfJson => Ok(format::format_schema_odf_json(&self.schema)),
            DataSchemaFormat::OdfYaml => Ok(format::format_schema_odf_yaml(&self.schema)),
            DataSchemaFormat::Parquet => {
                let parquet_schema = convert::arrow_schema_to_parquet_schema(
                    &self
                        .schema
                        .to_arrow(&odf::metadata::ToArrowSettings::default())
                        .int_err()?,
                );
                Ok(format::format_schema_parquet(&parquet_schema))
            }
            DataSchemaFormat::ParquetJson => {
                let parquet_schema = convert::arrow_schema_to_parquet_schema(
                    &self
                        .schema
                        .to_arrow(&odf::metadata::ToArrowSettings::default())
                        .int_err()?,
                );
                Ok(format::format_schema_parquet_json(&parquet_schema))
            }
        }
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

impl ColumnInput {
    pub fn int(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: DataTypeInput {
                // TODO: NOT NULL?
                ddl: "INT".into(),
            },
        }
    }

    pub fn string(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: DataTypeInput {
                // TODO: NOT NULL?
                ddl: "STRING".into(),
            },
        }
    }

    pub fn string_array(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: DataTypeInput {
                // TODO: NOT NULL?
                ddl: "ARRAY<STRING>".into(),
            },
        }
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

        let data_schema =
            DataSchema::new_from_data_frame_schema_stripped(df.schema(), DataSchemaFormat::Parquet)
                .unwrap();

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

        let data_schema = DataSchema::new_from_data_frame_schema_stripped(
            df.schema(),
            DataSchemaFormat::ParquetJson,
        )
        .unwrap();

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

        let data_schema = DataSchema::new_from_data_frame_schema_stripped(
            df.schema(),
            DataSchemaFormat::ParquetJson,
        )
        .unwrap();

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

        let data_schema = DataSchema::new_from_data_frame_schema_stripped(
            df.schema(),
            DataSchemaFormat::ArrowJson,
        )
        .unwrap();

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
            DataSchema::new_from_data_frame_schema_stripped(df.schema(), DataSchemaFormat::OdfJson)
                .unwrap();

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
                        "kind": "Int32",
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
            DataSchema::new_from_data_frame_schema_stripped(df.schema(), DataSchemaFormat::OdfYaml)
                .unwrap();

        assert_eq!(data_schema.format, DataSchemaFormat::OdfYaml);

        let expected_content = indoc::indoc!(
            r#"
            fields:
            - name: a
              type:
                kind: String
            - name: b
              type:
                kind: Int32
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
