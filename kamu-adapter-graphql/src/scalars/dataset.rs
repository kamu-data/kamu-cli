// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{convert::TryFrom, ops::Deref};

use async_graphql::*;
use opendatafabric as odf;

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetID(odf::DatasetID);

impl From<odf::DatasetID> for DatasetID {
    fn from(value: odf::DatasetID) -> Self {
        DatasetID(value)
    }
}

impl Into<odf::DatasetID> for DatasetID {
    fn into(self) -> odf::DatasetID {
        self.0
    }
}

impl Into<String> for DatasetID {
    fn into(self) -> String {
        self.0.to_did_string()
    }
}

impl Deref for DatasetID {
    type Target = odf::DatasetID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for DatasetID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetID::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DatasetName
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasetName(odf::DatasetName);

impl From<odf::DatasetName> for DatasetName {
    fn from(value: odf::DatasetName) -> Self {
        DatasetName(value)
    }
}

impl Into<odf::DatasetName> for DatasetName {
    fn into(self) -> odf::DatasetName {
        self.0
    }
}

impl Into<String> for DatasetName {
    fn into(self) -> String {
        self.0.into()
    }
}

impl Deref for DatasetName {
    type Target = odf::DatasetName;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for DatasetName {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetName::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

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
        schema: &datafusion::logical_plan::DFSchema,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        let parquet_schema =
            kamu::infra::utils::schema_utils::dataframe_schema_to_parquet_schema(schema);
        Self::from_parquet_schema(parquet_schema.as_ref(), format)
    }

    // TODO: Unify everything around Arrow schema
    pub fn from_parquet_schema(
        schema: &datafusion::parquet::schema::types::Type,
        format: DataSchemaFormat,
    ) -> Result<DataSchema> {
        use kamu::infra::utils::schema_utils;

        let mut buf = Vec::new();
        match format {
            DataSchemaFormat::Parquet => schema_utils::write_schema_parquet(&mut buf, &schema),
            DataSchemaFormat::ParquetJson => {
                schema_utils::write_schema_parquet_json(&mut buf, &schema)
            }
        }?;

        Ok(DataSchema {
            format,
            content: String::from_utf8(buf).unwrap(),
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DataBatch
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataBatchFormat {
    Json,
    JsonLD,
    JsonSOA,
    Csv,
}

#[derive(SimpleObject)]
pub struct DataBatch {
    pub format: DataBatchFormat,
    pub content: String,
    pub num_records: u64,
}

impl DataBatch {
    pub fn from_records(
        record_batches: &Vec<datafusion::arrow::record_batch::RecordBatch>,
        format: DataBatchFormat,
    ) -> Result<DataBatch> {
        use kamu::infra::utils::records_writers::*;

        let num_records: usize = record_batches.iter().map(|b| b.num_rows()).sum();

        let mut buf = Vec::new();
        {
            let mut writer: Box<dyn RecordsWriter> = match format {
                DataBatchFormat::Csv => {
                    Box::new(CsvWriterBuilder::new().has_headers(true).build(&mut buf))
                }
                DataBatchFormat::Json => Box::new(JsonArrayWriter::new(&mut buf)),
                DataBatchFormat::JsonLD => Box::new(JsonLineDelimitedWriter::new(&mut buf)),
                DataBatchFormat::JsonSOA => {
                    unimplemented!("SoA Json format is not yet implemented")
                }
            };

            writer.write_batches(record_batches)?;
            writer.finish()?;
        }

        Ok(DataBatch {
            format,
            content: String::from_utf8(buf).unwrap(),
            num_records: num_records as u64,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// DataQueryResult
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct DataQueryResult {
    pub schema: DataSchema,
    pub data: DataBatch,
    pub limit: u64,
}

/////////////////////////////////////////////////////////////////////////////////////////
// QueryDialect
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryDialect {
    DataFusion,
}
