// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;

/////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataSchemaFormat {
    Parquet,
    ParquetJson,
}

#[derive(SimpleObject)]
pub struct DataSchema {
    pub format: DataSchemaFormat,
    pub content: String,
}

impl DataSchema {
    pub fn from_data_frame_schema(
        schema: &datafusion::common::DFSchema,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        let parquet_schema =
            kamu_data_utils::schema::convert::dataframe_schema_to_parquet_schema(schema);
        Self::from_parquet_schema(parquet_schema.as_ref(), format)
    }

    // TODO: Unify everything around Arrow schema
    pub fn from_parquet_schema(
        schema: &datafusion::parquet::schema::types::Type,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        use kamu_data_utils::schema::format::*;

        let mut buf = Vec::new();
        match format {
            DataSchemaFormat::Parquet => write_schema_parquet(&mut buf, &schema),
            DataSchemaFormat::ParquetJson => write_schema_parquet_json(&mut buf, &schema),
        }?;

        Ok(DataSchema {
            format,
            content: String::from_utf8(buf).unwrap(),
        })
    }
}
