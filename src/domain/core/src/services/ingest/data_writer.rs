// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::SchemaRef;
use file_utils::OwnedFile;
use internal_error::*;
use odf::utils::data::DataFrameExt;

use super::MergeError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Auxiliary interface for appending data to root datasets.
/// Writers perform necessary transformations and merge strategies
/// to commit data into a dataset in bitemporal ledger form.
#[async_trait::async_trait]
pub trait DataWriter {
    // TODO: Avoid using Option<> and create empty DataFrame instead.
    // This would require us always knowing what the schema of data is (e.g. before
    // the first ingest run).
    async fn write(
        &mut self,
        new_data: Option<DataFrameExt>,
        opts: WriteDataOpts,
    ) -> Result<WriteDataResult, WriteDataError>;

    // A helper to advance the watermark only, without appending any new data.
    async fn write_watermark(
        &mut self,
        new_watermark: DateTime<Utc>,
        opts: WriteWatermarkOpts,
    ) -> Result<WriteDataResult, WriteWatermarkError>;

    /// Prepares all data for commit without actually committing
    async fn stage(
        &self,
        new_data: Option<DataFrameExt>,
        opts: WriteDataOpts,
    ) -> Result<StageDataResult, StageDataError>;

    /// Commit previously staged data and advance writer state
    async fn commit(
        &mut self,
        staged: StageDataResult,
    ) -> Result<WriteDataResult, odf::dataset::CommitError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WriteWatermarkOpts {
    /// Will be used for system time data column and metadata block timestamp
    pub system_time: DateTime<Utc>,
    /// Data source state to store in the commit
    pub new_source_state: Option<odf::metadata::SourceState>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct WriteDataOpts {
    /// Will be used for system time data column and metadata block timestamp
    pub system_time: DateTime<Utc>,
    /// If data does not contain event time column already this value will be
    /// used to populate it
    pub source_event_time: DateTime<Utc>,
    /// Explicit watermark to use in the commit
    pub new_watermark: Option<DateTime<Utc>>,
    /// Data source state to store in the commit
    pub new_source_state: Option<odf::metadata::SourceState>,
    // TODO: Find a better way to deal with temporary files
    /// Local FS path to which data slice will be written before committing it
    /// into the data object store of a dataset
    pub data_staging_path: PathBuf,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteDataResult {
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub add_data_block: Option<odf::MetadataBlockTyped<odf::metadata::AddData>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Do not create directly, only use with [`DataWriter::stage`].
#[derive(Debug)]
pub struct StageDataResult {
    pub system_time: DateTime<Utc>,
    /// Set when `SetDataSchema` event needs to be committed
    pub new_schema: Option<odf::schema::DataSchema>,
    /// Set when `AddData` event needs to be committed
    pub add_data: Option<odf::dataset::AddDataParams>,
    /// Set when commmit will contains some data
    pub data_file: Option<OwnedFile>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum WriteWatermarkError {
    #[error(transparent)]
    EmptyCommit(#[from] EmptyCommitError),

    #[error(transparent)]
    CommitError(#[from] odf::dataset::CommitError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum WriteDataError {
    #[error(transparent)]
    BadInputSchema(#[from] BadInputSchemaError),

    #[error(transparent)]
    IncompatibleSchema(#[from] IncompatibleSchemaError),

    #[error(transparent)]
    MergeError(#[from] MergeError),

    #[error(transparent)]
    DataValidation(#[from] DataValidationError),

    #[error(transparent)]
    EmptyCommit(#[from] EmptyCommitError),

    #[error(transparent)]
    CommitError(#[from] odf::dataset::CommitError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<StageDataError> for WriteDataError {
    fn from(value: StageDataError) -> Self {
        match value {
            StageDataError::BadInputSchema(v) => WriteDataError::BadInputSchema(v),
            StageDataError::IncompatibleSchema(v) => WriteDataError::IncompatibleSchema(v),
            StageDataError::MergeError(v) => WriteDataError::MergeError(v),
            StageDataError::DataValidation(v) => WriteDataError::DataValidation(v),
            StageDataError::EmptyCommit(v) => WriteDataError::EmptyCommit(v),
            StageDataError::Internal(v) => WriteDataError::Internal(v),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum StageDataError {
    #[error(transparent)]
    BadInputSchema(#[from] BadInputSchemaError),

    #[error(transparent)]
    IncompatibleSchema(#[from] IncompatibleSchemaError),

    #[error(transparent)]
    MergeError(#[from] MergeError),

    #[error(transparent)]
    DataValidation(#[from] DataValidationError),

    #[error(transparent)]
    EmptyCommit(#[from] EmptyCommitError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub struct BadInputSchemaError {
    pub schema: SchemaRef,
    message: String,
    backtrace: Backtrace,
}

impl BadInputSchemaError {
    pub fn new(message: impl Into<String>, schema: SchemaRef) -> Self {
        Self {
            schema,
            message: message.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for BadInputSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "Bad input schema: {}\n{}",
            self.message,
            FmtSchema(&self.schema)
        )?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum DataValidationError {
    #[error(transparent)]
    InvalidValue(#[from] InvalidValueError),

    #[error(transparent)]
    DanglingReference(#[from] DanglingReferenceError),
}

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct InvalidValueError {
    message: String,
    backtrace: Backtrace,
}

impl InvalidValueError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("ObjectLink column references a non-existing object {hash}")]
pub struct DanglingReferenceError {
    pub hash: odf::Multihash,
    backtrace: Backtrace,
}

impl DanglingReferenceError {
    pub fn new(hash: odf::Multihash) -> Self {
        Self {
            hash,
            backtrace: Backtrace::capture(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub struct IncompatibleSchemaError {
    pub prev_schema: SchemaRef,
    pub new_schema: SchemaRef,
    message: String,
    backtrace: Backtrace,
}

impl IncompatibleSchemaError {
    pub fn new(message: impl Into<String>, prev_schema: SchemaRef, new_schema: SchemaRef) -> Self {
        Self {
            prev_schema,
            new_schema,
            message: message.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for IncompatibleSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Incompatible schema: {}", self.message)?;
        writeln!(f, "Dataset schema:\n{}", FmtSchema(&self.prev_schema))?;
        writeln!(f, "New slice schema:\n{}", FmtSchema(&self.new_schema))?;
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Nothing to commit")]
pub struct EmptyCommitError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FmtSchema<'a>(&'a SchemaRef);

impl std::fmt::Display for FmtSchema<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parquet_schema = datafusion::parquet::arrow::ArrowSchemaConverter::new()
            .convert(self.0)
            .unwrap();

        let mut buf = Vec::new();
        datafusion::parquet::schema::printer::print_schema(&mut buf, parquet_schema.root_schema());
        let schema = String::from_utf8(buf).unwrap();

        write!(f, "{schema}")
    }
}
