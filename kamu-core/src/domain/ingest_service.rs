// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::*;

use std::backtrace::Backtrace;
use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait IngestService: Send + Sync {
    async fn ingest(
        &self,
        dataset_ref: &DatasetRefLocal,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRefLocal,
        fetch: FetchStep,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    async fn ingest_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefLocal>,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)>;

    async fn ingest_multi_ext(
        &self,
        requests: &mut dyn Iterator<Item = IngestRequest>,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetRefLocal, Result<IngestResult, IngestError>)>;
}

#[derive(Clone, Debug)]
pub struct IngestRequest {
    pub dataset_ref: DatasetRefLocal,
    pub fetch_override: Option<FetchStep>,
}

#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Fetch latest data from uncacheable data sources
    pub fetch_uncacheable: bool,
    /// Pull sources that yield multiple data files until they are
    /// fully exhausted
    pub exhaust_sources: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            fetch_uncacheable: false,
            exhaust_sources: false,
        }
    }
}

#[derive(Debug)]
pub enum IngestResult {
    UpToDate {
        uncacheable: bool,
        has_more: bool,
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

pub trait IngestListener: Send + Sync {
    fn begin(&self) {}
    fn on_stage_progress(&self, _stage: IngestStage, _n: u64, _out_of: u64) {}

    fn success(&self, _result: &IngestResult) {}
    fn error(&self, _error: &IngestError) {}

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub struct NullIngestListener;
impl IngestListener for NullIngestListener {}

pub trait IngestMultiListener {
    fn begin_ingest(&self, _dataset: &DatasetHandle) -> Option<Arc<dyn IngestListener>> {
        None
    }
}

pub struct NullIngestMultiListener;
impl IngestMultiListener for NullIngestMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

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
    #[error("Pipe command error: {command:?} {source}")]
    PipeError {
        command: Vec<String>,
        source: BoxedError,
        backtrace: Backtrace,
    },
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

impl IngestError {
    pub fn unreachable(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        IngestError::Unreachable {
            path: path.into(),
            source: source,
        }
    }

    pub fn not_found(path: impl Into<String>, source: Option<BoxedError>) -> Self {
        IngestError::NotFound {
            path: path.into(),
            source: source,
        }
    }

    pub fn pipe(command: Vec<String>, e: impl std::error::Error + Send + Sync + 'static) -> Self {
        IngestError::PipeError {
            command: command,
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
