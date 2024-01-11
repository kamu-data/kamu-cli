// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use container_runtime::ImagePullError;
use opendatafabric::*;
use thiserror::Error;

use crate::engine::{EngineError, ProcessError};
use crate::*;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PollingIngestService: Send + Sync {
    /// Returns an active polling source, if any
    async fn get_active_polling_source(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<(Multihash, MetadataBlockTyped<SetPollingSource>)>, GetDatasetError>;

    /// Uses polling source definition in metadata to ingest data from an
    /// external source
    async fn ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: PollingIngestOptions,
        listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestError>;

    /// A batch version of [PollingIngestService::ingest]
    async fn ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: PollingIngestOptions,
        listener: Option<Arc<dyn PollingIngestMultiListener>>,
    ) -> Vec<PollingIngestResponse>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct PollingIngestOptions {
    /// Fetch latest data from uncacheable data sources
    pub fetch_uncacheable: bool,
    /// Pull sources that yield multiple data files until they are
    /// fully exhausted
    pub exhaust_sources: bool,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PollingIngestResponse {
    pub dataset_ref: DatasetRef,
    pub result: Result<PollingIngestResult, PollingIngestError>,
}

#[derive(Debug)]
pub enum PollingIngestResult {
    UpToDate {
        no_source_defined: bool,
        uncacheable: bool,
    },
    Updated {
        old_head: Multihash,
        new_head: Multihash,
        num_blocks: usize,
        has_more: bool,
        uncacheable: bool,
    },
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingIngestStage {
    CheckCache,
    Fetch,
    Prepare,
    Read,
    Preprocess,
    Merge,
    Commit,
}

#[allow(unused_variables)]
pub trait PollingIngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_cache_hit(&self, created_at: &DateTime<Utc>) {}
    fn on_stage_progress(&self, stage: PollingIngestStage, _progress: u64, _out_of: TotalSteps) {}
    fn success(&self, result: &PollingIngestResult) {}
    fn error(&self, error: &PollingIngestError) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub enum TotalSteps {
    Unknown,
    Exact(u64),
}

pub struct NullPollingIngestListener;
impl PollingIngestListener for NullPollingIngestListener {}

pub trait PollingIngestMultiListener: Send + Sync {
    fn begin_ingest(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn PollingIngestListener>> {
        None
    }
}

pub struct NullPollingIngestMultiListener;
impl PollingIngestMultiListener for NullPollingIngestMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Environment variable {name} not set")]
pub struct IngestParameterNotFound {
    pub name: String,
}

impl IngestParameterNotFound {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Error, Debug)]
#[error("Invalid environment variable {name} format '{value}'")]
pub struct InvalidIngestParameterFormat {
    pub name: String,
    pub value: String,
}

impl InvalidIngestParameterFormat {
    pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

// TODO: Revisit error granularity
#[derive(Debug, Error)]
pub enum PollingIngestError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),

    #[error("Source is unreachable at {path}")]
    Unreachable {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },

    #[error("Source not found at {path}")]
    NotFound {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },

    #[error(transparent)]
    ImagePull(
        #[from]
        #[backtrace]
        ImagePullError,
    ),

    #[error(transparent)]
    ParameterNotFound(
        #[from]
        #[backtrace]
        IngestParameterNotFound,
    ),

    #[error(transparent)]
    ProcessError(
        #[from]
        #[backtrace]
        ProcessError,
    ),

    #[error("Pipe command error: {command:?} {source}")]
    PipeError {
        command: Vec<String>,
        source: BoxedError,
        backtrace: Backtrace,
    },

    #[error(transparent)]
    ReadError(
        #[from]
        #[backtrace]
        ReadError,
    ),

    #[error("Engine provisioning error")]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),

    #[error("Engine error")]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
    ),

    #[error(transparent)]
    BadInputSchema(
        #[from]
        #[backtrace]
        BadInputSchemaError,
    ),

    #[error(transparent)]
    IncompatibleSchema(
        #[from]
        #[backtrace]
        IncompatibleSchemaError,
    ),

    #[error(transparent)]
    MergeError(
        #[from]
        #[backtrace]
        MergeError,
    ),

    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        CommitError,
    ),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),

    #[error(transparent)]
    InvalidParameterFormat(
        #[from]
        #[backtrace]
        InvalidIngestParameterFormat,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for PollingIngestError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for PollingIngestError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl PollingIngestError {
    pub fn unreachable(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        Self::Unreachable {
            path: path.into(),
            source,
        }
    }

    pub fn not_found(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        Self::NotFound {
            path: path.into(),
            source,
        }
    }

    pub fn pipe(command: Vec<String>, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::PipeError {
            command,
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
