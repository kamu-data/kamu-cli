use crate::domain::*;
use crate::infra::serde::yaml::formats::datetime_rfc3339_opt;
use crate::infra::serde::yaml::*;

use ::serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use serde_with::skip_serializing_none;
use std::backtrace::Backtrace;
use std::path::PathBuf;
use thiserror::Error;

pub trait Engine {
    fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError>;
}

///////////////////////////////////////////////////////////////////////////////
// Request / Response DTOs
///////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestRequest {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetIDBuf,
    pub ingest_path: PathBuf,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub event_time: Option<DateTime<Utc>>,
    pub source: DatasetSourceRoot,
    pub dataset_vocab: DatasetVocabulary,
    pub checkpoints_dir: PathBuf,
    pub data_dir: PathBuf,
}

#[skip_serializing_none]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResponse {
    pub block: MetadataBlock,
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Engine {id} was not found")]
    NotFound { id: String },
    #[error("{source}")]
    IOError {
        #[from]
        source: std::io::Error,
        #[backtrace]
        backtrace: Backtrace,
    },
    #[error("Process error: {0}")]
    ProcessError(#[from] ProcessError),
    #[error("Contract error: {0}")]
    ContractError(#[from] ContractError),
    #[error("Internal error: {source}")]
    InternalError {
        #[from]
        source: Box<dyn std::error::Error + Send + Sync>,
        #[backtrace]
        backtrace: Backtrace,
    },
}

#[derive(Debug, Error)]
pub struct ProcessError {
    exit_code: Option<i32>,
    backtrace: Backtrace,
}

#[derive(Debug, Error)]
pub struct ContractError {
    reason: String,
    backtrace: Backtrace,
}

impl EngineError {
    pub fn not_found(id: &str) -> Self {
        EngineError::NotFound { id: id.to_owned() }
    }

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        EngineError::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl ProcessError {
    pub fn new(exit_code: Option<i32>) -> Self {
        Self {
            exit_code: exit_code,
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.exit_code {
            Some(c) => write!(f, "Process exited with code {}", c),
            None => write!(f, "Process terminated by a signal"),
        }
    }
}

impl ContractError {
    pub fn new(reason: &str) -> Self {
        Self {
            reason: reason.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)
    }
}
