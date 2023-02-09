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
use datafusion::error::DataFusionError;
use kamu::domain::QueryError;
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
        schema: &datafusion::common::DFSchema,
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
    pub fn empty(format: DataBatchFormat) -> DataBatch {
        DataBatch {
            format,
            content: match format {
                DataBatchFormat::Csv => String::from(""),
                DataBatchFormat::Json |
                DataBatchFormat::JsonLD |
                DataBatchFormat::JsonSOA => String::from("{}"),
            },
            num_records: 0
        }
    }

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
                DataBatchFormat::Json => {
                    // HACK: JsonArrayWriter should be producing [] when there are no rows
                    if num_records != 0 {
                        Box::new(JsonArrayWriter::new(&mut buf))
                    } else {
                        return Ok(DataBatch {
                            format,
                            content: "[]".to_string(),
                            num_records: num_records as u64,
                        });
                    }
                }
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
pub struct DataQueryResultSuccess {
    pub schema: Option<DataSchema>,
    pub data: DataBatch,
    pub limit: u64,
}

#[derive(SimpleObject)]
pub struct DataQueryResultError {
    pub error_message: String,
    pub error_kind: DataQueryResultErrorKind,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataQueryResultErrorKind {
    InvalidSql,
    InternalError,
}

#[derive(Union)]
pub enum DataQueryResult {
    Success(DataQueryResultSuccess),
    Error(DataQueryResultError),
}

impl DataQueryResult {
    pub fn success(schema: Option<DataSchema>, data: DataBatch, limit: u64) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema,
            data,
            limit,
        })
    }

    pub fn no_schema_yet(format: DataBatchFormat, limit: u64) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema: None, 
            data: DataBatch::empty(format), 
            limit,
        })
    }

    pub fn invalid_sql(error_message: String) -> DataQueryResult {
        DataQueryResult::Error(DataQueryResultError {
            error_message,
            error_kind: DataQueryResultErrorKind::InvalidSql,
        })
    }

    pub fn internal(error_message: String) -> DataQueryResult {
        DataQueryResult::Error(DataQueryResultError {
            error_message,
            error_kind: DataQueryResultErrorKind::InternalError,
        })
    }
    
}

impl From<QueryError> for DataQueryResult {
    fn from(e: QueryError) -> Self {
        match e {
            QueryError::DatasetNotFound(e) => DataQueryResult::invalid_sql(e.to_string()),
            QueryError::DataFusionError(e) => e.into(),
            QueryError::DatasetSchemaNotAvailable(_) => unreachable!(),
            QueryError::Internal(e) => DataQueryResult::internal(e.to_string()),
        }
    }
}

impl From<DataFusionError> for DataQueryResult {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::SQL(e) => DataQueryResult::invalid_sql(e.to_string()),
            DataFusionError::Plan(e) => DataQueryResult::invalid_sql(e),
            _ => DataQueryResult::internal(e.to_string()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// QueryDialect
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryDialect {
    DataFusion,
}
