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
use tokio::io::AsyncRead;

use super::ingest;
use crate::engine::{EngineError, ProcessError};
use crate::*;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait IngestService: Send + Sync {
    /// Uses polling source definition in metadata to ingest data from an
    /// external source
    async fn polling_ingest(
        &self,
        dataset_ref: &DatasetRef,
        options: PollingIngestOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    /// A batch version of [IngestService::polling_ingest]
    async fn polling_ingest_multi(
        &self,
        dataset_refs: Vec<DatasetRef>,
        options: PollingIngestOptions,
        listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<IngestResponse>;

    /// Uses push source defenition in metadata to ingest data from the
    /// specified source
    async fn push_ingest_from_url(
        &self,
        dataset_ref: &DatasetRef,
        data_url: url::Url,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    /// Uses push source defenition in metadata to ingest data passessed in-band
    async fn push_ingest_from_stream(
        &self,
        dataset_ref: &DatasetRef,
        data: Box<dyn AsyncRead + Send + Unpin>,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;
}

#[derive(Debug, Clone)]
pub struct PollingIngestOptions {
    /// Fetch latest data from uncacheable data sources
    pub fetch_uncacheable: bool,
    /// Pull sources that yield multiple data files until they are
    /// fully exhausted
    pub exhaust_sources: bool,
}

impl Default for PollingIngestOptions {
    fn default() -> Self {
        Self {
            fetch_uncacheable: false,
            exhaust_sources: false,
        }
    }
}

#[derive(Debug)]
pub struct IngestResponse {
    pub dataset_ref: DatasetRef,
    pub result: Result<IngestResult, IngestError>,
}

#[derive(Debug)]
pub enum IngestResult {
    UpToDate {
        no_polling_source: bool,
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
pub enum IngestStage {
    CheckCache,
    Fetch,
    Prepare,
    Read,
    Preprocess,
    Merge,
    Commit,
}

#[allow(unused_variables)]
pub trait IngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_cache_hit(&self, created_at: &DateTime<Utc>) {}
    fn on_stage_progress(&self, stage: IngestStage, _progress: u64, _out_of: TotalSteps) {}
    fn success(&self, result: &IngestResult) {}
    fn error(&self, error: &IngestError) {}

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

pub struct NullIngestListener;
impl IngestListener for NullIngestListener {}

pub trait IngestMultiListener: Send + Sync {
    fn begin_ingest(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn IngestListener>> {
        None
    }
}

pub struct NullIngestMultiListener;
impl IngestMultiListener for NullIngestMultiListener {}

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

// TODO: Revisit error granularity
#[derive(Debug, Error)]
pub enum IngestError {
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
        ingest::ReadError,
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
    MergeError(
        #[from]
        #[backtrace]
        ingest::MergeError,
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
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for IngestError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<auth::DatasetActionUnauthorizedError> for IngestError {
    fn from(v: auth::DatasetActionUnauthorizedError) -> Self {
        match v {
            auth::DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            auth::DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

impl IngestError {
    pub fn unreachable(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        IngestError::Unreachable {
            path: path.into(),
            source,
        }
    }

    pub fn not_found(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        IngestError::NotFound {
            path: path.into(),
            source,
        }
    }

    pub fn pipe(command: Vec<String>, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        IngestError::PipeError {
            command,
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
