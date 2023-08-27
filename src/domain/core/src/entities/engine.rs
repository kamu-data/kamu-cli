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
use internal_error::*;
use opendatafabric::*;
use thiserror::Error;

use crate::OwnedFile;

///////////////////////////////////////////////////////////////////////////////
// Engine
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Engine: Send + Sync {
    async fn transform(&self, request: TransformRequest) -> Result<TransformResponse, EngineError>;
}

// TODO: This interface is temporary and will be removed when ingestion is moved
// from Spark into Kamu
#[async_trait::async_trait]
pub trait IngestEngine: Send + Sync {
    async fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError>;
}

///////////////////////////////////////////////////////////////////////////////
// Request / Response DTOs
///////////////////////////////////////////////////////////////////////////////

/// A request for ingesting new data into a root dataset.
///
/// Design notes: This DTO is formed as an intermediate between analyzing
/// metadata chain and passing a final request to an engine. It contains
/// enough information to define the entire ingest operation so that no extra
/// interaction with metadata chain was needed, but it still operates with
/// higher-level types. This request will be resolved into physical data
/// locations before passing it to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRequest {
    /// Randomly assigned value that identifies this specific engine operation
    pub operation_id: String,
    /// Identifies the output dataset
    pub dataset_handle: DatasetHandle,
    /// Polling source
    pub polling_source: SetPollingSource,
    /// System time to use for new records
    pub system_time: DateTime<Utc>,
    /// Event time extracted from source's metadata
    pub event_time: Option<DateTime<Utc>>,
    /// Starting offset to use for new records
    pub next_offset: i64,
    /// Output dataset's vocabulary
    pub vocab: DatasetVocabulary,
    /// List of *all* previous data files (needed for merge step)
    pub prev_data_slices: Vec<Multihash>,
    /// Previous checkpoint, if any
    pub prev_checkpoint: Option<Multihash>,
    /// Previous watermark, if any
    pub prev_watermark: Option<DateTime<Utc>>,
    /// Previous source state, if any
    pub prev_source_state: Option<SourceState>,
    /// Host path to raw data to read
    pub input_data_path: PathBuf,
}

#[derive(Debug)]
pub struct IngestResponse {
    /// Data slice produced by the transaction, if any
    pub data_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any
    pub output_watermark: Option<DateTime<Utc>>,
    /// New checkpoint written by the engine, if any
    pub out_checkpoint: Option<OwnedFile>,
    /// Data produced by the operation, if any
    pub out_data: Option<OwnedFile>,
}

///////////////////////////////////////////////////////////////////////////////

/// A request for derivative (streaming) transformation.
///
/// Design notes: This DTO is formed as an intermediate between analyzing
/// metadata chain and passing a final request to an engine. It contains
/// enough information to define the entire transform operation so that no extra
/// interaction with metadata chain was needed, but it still operates with
/// higher-level types. This request will be resolved into physical data
/// locations before passing it to the engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformRequest {
    /// Randomly assigned value that identifies this specific engine operation
    pub operation_id: String,
    /// Identifies the output dataset
    pub dataset_handle: DatasetHandle,
    /// Transformation that will be applied to produce new data
    pub transform: Transform,
    /// System time to use for new records
    pub system_time: DateTime<Utc>,
    /// Starting offset to use for new records
    pub next_offset: i64,
    /// Defines the input data
    pub inputs: Vec<TransformRequestInput>,
    /// Output dataset's vocabulary
    pub vocab: DatasetVocabulary,
    /// Previous checkpoint, if any
    pub prev_checkpoint: Option<Multihash>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransformRequestInput {
    /// Identifies the input dataset
    pub dataset_handle: DatasetHandle,
    /// An alias of this input to be used in queries
    pub alias: String,
    /// Input dataset's vocabulary
    pub vocab: DatasetVocabulary,
    /// Blocks that went into this transaction
    pub block_interval: Option<BlockInterval>,
    /// Data that went into this transaction
    pub data_interval: Option<OffsetInterval>,
    /// List of data files that will be read
    pub data_slices: Vec<Multihash>,
    /// TODO: replace with actual schema
    pub schema_slice: Multihash,
    /// TODO: remove?
    pub explicit_watermarks: Vec<Watermark>,
}

#[derive(Debug)]
pub struct TransformResponse {
    /// Data slice produced by the transaction, if any
    pub data_interval: Option<OffsetInterval>,
    /// Watermark advanced by the transaction, if any
    pub output_watermark: Option<DateTime<Utc>>,
    /// New checkpoint written by the engine, if any
    pub out_checkpoint: Option<OwnedFile>,
    /// Data produced by the operation, if any
    pub out_data: Option<OwnedFile>,
}

impl Into<InputSlice> for TransformRequestInput {
    fn into(self) -> InputSlice {
        InputSlice {
            dataset_id: self.dataset_handle.id,
            block_interval: self.block_interval,
            data_interval: self.data_interval,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineError {
    #[error(transparent)]
    InvalidQuery(#[from] InvalidQueryError),
    #[error(transparent)]
    ProcessError(#[from] ProcessError),
    #[error(transparent)]
    ContractError(#[from] ContractError),
    #[error(transparent)]
    InternalError(#[from] InternalEngineError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct InvalidQueryError {
    pub message: String,
    pub log_files: Vec<PathBuf>,
    pub backtrace: Backtrace,
}

impl InvalidQueryError {
    pub fn new(message: impl Into<String>, log_files: Vec<PathBuf>) -> Self {
        Self {
            message: message.into(),
            log_files,
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for InvalidQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid query: {}", self.message)?;

        if self.log_files.len() != 0 {
            write!(f, "\nSee log files for details:\n")?;
            for path in self.log_files.iter() {
                write!(f, "- {}\n", path.display())?;
            }
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct ProcessError {
    pub exit_code: Option<i32>,
    pub log_files: Vec<PathBuf>,
    pub backtrace: Backtrace,
}

impl ProcessError {
    pub fn new(exit_code: Option<i32>, log_files: Vec<PathBuf>) -> Self {
        Self {
            exit_code,
            log_files: normalize_logs(log_files),
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Process error: ")?;

        match self.exit_code {
            Some(c) => write!(f, "Process exited with code {}", c)?,
            None => write!(f, "Process terminated by a signal")?,
        }

        if self.log_files.len() != 0 {
            write!(f, ", see log files for details:\n")?;
            for path in self.log_files.iter() {
                write!(f, "- {}\n", path.display())?;
            }
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct ContractError {
    pub reason: String,
    pub log_files: Vec<PathBuf>,
    pub backtrace: Backtrace,
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Contract error: {}", self.reason)?;

        if self.log_files.len() != 0 {
            write!(f, ", see log files for details:\n")?;
            for path in self.log_files.iter() {
                write!(f, "- {}\n", path.display())?;
            }
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct InternalEngineError {
    #[source]
    pub source: InternalError,
    pub log_files: Vec<PathBuf>,
}

impl std::fmt::Display for InternalEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.log_files.len() != 0 {
            write!(f, "Internal engine error, see log files for details:")?;
            for path in self.log_files.iter() {
                write!(f, "\n- {}", path.display())?;
            }
        }

        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

impl EngineError {
    pub fn invalid_query(message: impl Into<String>, log_files: Vec<PathBuf>) -> Self {
        EngineError::InvalidQuery(InvalidQueryError {
            message: message.into(),
            log_files: normalize_logs(log_files),
            backtrace: Backtrace::capture(),
        })
    }

    pub fn process_error(exit_code: Option<i32>, log_files: Vec<PathBuf>) -> Self {
        Self::ProcessError(ProcessError::new(exit_code, log_files))
    }

    pub fn contract_error(reason: &str, log_files: Vec<PathBuf>) -> Self {
        Self::ContractError(ContractError {
            reason: reason.to_owned(),
            log_files: normalize_logs(log_files),
            backtrace: Backtrace::capture(),
        })
    }

    pub fn internal(e: impl Into<BoxedError>, log_files: Vec<PathBuf>) -> Self {
        EngineError::InternalError(InternalEngineError {
            source: InternalError::new(e),
            log_files: normalize_logs(log_files),
        })
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e, Vec::new())
    }
}

impl From<InternalError> for EngineError {
    fn from(e: InternalError) -> Self {
        Self::InternalError(InternalEngineError {
            source: e,
            log_files: Vec::new(),
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

fn normalize_logs(log_files: Vec<PathBuf>) -> Vec<PathBuf> {
    let cwd = std::env::current_dir().unwrap_or_default();
    log_files
        .into_iter()
        .filter(|p| match std::fs::metadata(p) {
            Ok(m) => m.len() > 0,
            Err(_) => true,
        })
        .map(|p| {
            if let Some(relpath) = pathdiff::diff_paths(&p, &cwd) {
                if relpath.as_os_str().len() < p.as_os_str().len() {
                    relpath
                } else {
                    p
                }
            } else {
                p
            }
        })
        .collect()
}
