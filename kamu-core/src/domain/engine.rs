// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::serde::yaml::*;
use opendatafabric::*;

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::backtrace::Backtrace;
use std::path::PathBuf;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Engine
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Engine: Send + Sync {
    async fn transform(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError>;
}

// TODO: This interface is temporary and will be removed when ingestion is moved from Spark into Kamu
#[async_trait::async_trait]
pub trait IngestEngine: Send + Sync {
    async fn ingest(
        &self,
        request: IngestRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError>;
}

///////////////////////////////////////////////////////////////////////////////
// Request / Response DTOs
///////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestRequest {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetID,
    pub dataset_name: DatasetName,
    pub ingest_path: PathBuf,
    #[serde(with = "datetime_rfc3339")]
    pub system_time: DateTime<Utc>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub event_time: Option<DateTime<Utc>>,
    pub offset: i64,
    #[serde(with = "SetPollingSourceDef")]
    pub source: SetPollingSource,
    #[serde(with = "DatasetVocabularyDef")]
    pub dataset_vocab: DatasetVocabulary,
    #[serde(skip)]
    pub prev_watermark: Option<DateTime<Utc>>,
    pub prev_checkpoint_path: Option<PathBuf>,
    pub new_checkpoint_path: PathBuf,
    pub data_dir: PathBuf,
    pub out_data_path: PathBuf,
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    InvalidQuery(#[from] InvalidQueryError),
    #[error("{0}")]
    ProcessError(#[from] ProcessError),
    #[error("{0}")]
    ContractError(#[from] ContractError),
    #[error("{0}")]
    InternalError(#[from] InternalEngineError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub struct InvalidQueryError {
    pub message: String,
    pub log_files: Vec<PathBuf>,
    pub backtrace: Backtrace,
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
    pub source: Box<dyn std::error::Error + Send + Sync>,
    pub log_files: Vec<PathBuf>,
    #[backtrace]
    pub backtrace: Backtrace,
}

impl std::fmt::Display for InternalEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Internal error: {}", self.source)?;

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

    pub fn internal<E>(e: E, log_files: Vec<PathBuf>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        EngineError::InternalError(InternalEngineError {
            source: e.into(),
            log_files: normalize_logs(log_files),
            backtrace: Backtrace::capture(),
        })
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e, Vec::new())
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
        .map(|p| pathdiff::diff_paths(&p, &cwd).unwrap_or(p))
        .collect()
}
