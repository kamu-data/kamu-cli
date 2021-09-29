// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::serde::yaml::formats::datetime_rfc3339_opt;
use opendatafabric::serde::yaml::generated::*;
use opendatafabric::{
    DatasetIDBuf, DatasetSourceRoot, DatasetVocabulary, ExecuteQueryRequest,
    ExecuteQueryResponseSuccess,
};

use ::serde::{Deserialize, Serialize};
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::backtrace::Backtrace;
use std::path::PathBuf;
use thiserror::Error;

pub trait Engine: Sync + Send {
    fn transform(
        &self,
        request: ExecuteQueryRequest,
    ) -> Result<ExecuteQueryResponseSuccess, EngineError>;
}

// TODO: This interface is temporary and will be removed when ingestion is moved from Spark into Kamu
pub trait IngestEngine: Sync + Send {
    fn ingest(&self, request: IngestRequest) -> Result<ExecuteQueryResponseSuccess, EngineError>;
}

///////////////////////////////////////////////////////////////////////////////
// Request / Response DTOs
///////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestRequest {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetIDBuf,
    pub ingest_path: PathBuf,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub event_time: Option<DateTime<Utc>>,
    #[serde(with = "DatasetSourceRootDef")]
    pub source: DatasetSourceRoot,
    #[serde(with = "DatasetVocabularyDef")]
    pub dataset_vocab: DatasetVocabulary,
    pub prev_checkpoint_dir: Option<PathBuf>,
    pub new_checkpoint_dir: PathBuf,
    pub data_dir: PathBuf,
    pub out_data_path: PathBuf,
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("{0}")]
    ImageNotFound(#[from] ImageNotFoundError),
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
#[error("Image not found: {image_name}")]
pub struct ImageNotFoundError {
    pub image_name: String,
    pub backtrace: Backtrace,
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
    pub fn image_not_found(image_name: &str) -> Self {
        EngineError::ImageNotFound(ImageNotFoundError {
            image_name: image_name.to_owned(),
            backtrace: Backtrace::capture(),
        })
    }

    pub fn invalid_query(message: impl Into<String>, log_files: Vec<PathBuf>) -> Self {
        EngineError::InvalidQuery(InvalidQueryError {
            message: message.into(),
            backtrace: Backtrace::capture(),
            log_files: Self::normalize_logs(log_files),
        })
    }

    pub fn process_error(exit_code: Option<i32>, log_files: Vec<PathBuf>) -> Self {
        Self::ProcessError(ProcessError {
            exit_code,
            backtrace: Backtrace::capture(),
            log_files: Self::normalize_logs(log_files),
        })
    }

    pub fn contract_error(reason: &str, log_files: Vec<PathBuf>) -> Self {
        Self::ContractError(ContractError {
            reason: reason.to_owned(),
            backtrace: Backtrace::capture(),
            log_files: Self::normalize_logs(log_files),
        })
    }

    pub fn internal<E>(e: E, log_files: Vec<PathBuf>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        EngineError::InternalError(InternalEngineError {
            source: e.into(),
            backtrace: Backtrace::capture(),
            log_files: Self::normalize_logs(log_files),
        })
    }

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
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e, Vec::new())
    }
}
