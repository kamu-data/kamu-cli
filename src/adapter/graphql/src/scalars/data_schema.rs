// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_data_utils::schema::format::*;
use serde_json::to_string;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////
// DataSchema
/////////////////////////////////////////////////////////////////////////////////////////

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
    pub fn from_arrow_schema(schema: &datafusion::arrow::datatypes::Schema) -> DataSchema {
        let content = to_string(schema).unwrap();
        DataSchema {
            format: DataSchemaFormat::ArrowJson,
            content,
        }
    }

    pub fn from_data_frame_schema(
        schema: &datafusion::common::DFSchema,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        let mut buf = Vec::new();
        match format {
            DataSchemaFormat::Parquet => write_schema_parquet(
                &mut buf,
                &kamu_data_utils::schema::convert::dataframe_schema_to_parquet_schema(schema),
            ),
            DataSchemaFormat::ParquetJson => write_schema_parquet_json(
                &mut buf,
                &kamu_data_utils::schema::convert::dataframe_schema_to_parquet_schema(schema),
            ),
            DataSchemaFormat::ArrowJson => {
                let arrow_schema = datafusion::arrow::datatypes::Schema::from(schema);
                write_schema_arrow_json(&mut buf, &arrow_schema)
            }
        }
        .int_err()?;

        Ok(DataSchema {
            format,
            content: String::from_utf8(buf).unwrap(),
        })
    }
}
