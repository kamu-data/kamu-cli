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
    DatasetIDBuf, DatasetSourceDerivative, DatasetSourceRoot, DatasetVocabulary, MetadataBlock,
    TimeInterval, Watermark,
};

use ::serde::{Deserialize, Serialize};
use ::serde_with::serde_as;
use ::serde_with::skip_serializing_none;
use chrono::{DateTime, Utc};
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::path::PathBuf;
use thiserror::Error;

pub trait Engine {
    fn transform(&self, request: ExecuteQueryRequest) -> Result<ExecuteQueryResponse, EngineError>;
}

// TODO: This interface is temporary and will be removed when ingestion is moved from Spark into Kamu
pub trait IngestEngine {
    fn ingest(&self, request: IngestRequest) -> Result<IngestResponse, EngineError>;
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

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestResponse {
    #[serde(with = "MetadataBlockDef")]
    pub block: MetadataBlock,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryRequest {
    #[serde(rename = "datasetID")]
    pub dataset_id: DatasetIDBuf,
    #[serde(with = "DatasetSourceDerivativeDef")]
    pub source: DatasetSourceDerivative,
    #[serde_as(as = "BTreeMap<_, DatasetVocabularyDef>")]
    pub dataset_vocabs: BTreeMap<DatasetIDBuf, DatasetVocabulary>,
    pub input_slices: BTreeMap<DatasetIDBuf, InputDataSlice>,
    pub prev_checkpoint_dir: Option<PathBuf>,
    pub new_checkpoint_dir: PathBuf,
    pub out_data_path: PathBuf,
}

impl ExecuteQueryRequest {
    pub fn is_empty(&self) -> bool {
        self.input_slices.values().all(|s| s.is_empty())
    }
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ExecuteQueryResponse {
    #[serde(with = "MetadataBlockDef")]
    pub block: MetadataBlock,
}

#[skip_serializing_none]
#[serde_as]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct InputDataSlice {
    pub interval: TimeInterval,
    pub data_paths: Vec<PathBuf>,
    // TODO: Replace with just DDL schema
    pub schema_file: PathBuf,
    #[serde_as(as = "Vec<WatermarkDef>")]
    pub explicit_watermarks: Vec<Watermark>,
}

impl InputDataSlice {
    pub fn is_empty(&self) -> bool {
        self.data_paths.is_empty() && self.explicit_watermarks.is_empty()
    }
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("Engine image not found: {image_name}")]
    ImageNotFound {
        image_name: String,
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
    log_files: Vec<PathBuf>,
    backtrace: Backtrace,
}

#[derive(Debug, Error)]
pub struct ContractError {
    reason: String,
    log_files: Vec<PathBuf>,
    backtrace: Backtrace,
}

impl EngineError {
    pub fn image_not_found(image_name: &str) -> Self {
        EngineError::ImageNotFound {
            image_name: image_name.to_owned(),
            backtrace: Backtrace::capture(),
        }
    }

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        EngineError::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

impl From<std::io::Error> for EngineError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e)
    }
}

impl ProcessError {
    pub fn new(exit_code: Option<i32>, log_files: Vec<PathBuf>) -> Self {
        Self {
            exit_code: exit_code,
            log_files: log_files,
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
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

impl ContractError {
    pub fn new(reason: &str, log_files: Vec<PathBuf>) -> Self {
        Self {
            reason: reason.to_owned(),
            log_files: log_files,
            backtrace: Backtrace::capture(),
        }
    }
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.reason)?;

        if self.log_files.len() != 0 {
            write!(f, ", see log files for details:\n")?;
            for path in self.log_files.iter() {
                write!(f, "- {}\n", path.display())?;
            }
        }

        Ok(())
    }
}
