// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::EngineError;
use container_runtime::PullImageListener;
use opendatafabric::{DatasetID, DatasetIDBuf, FetchStep, Sha3_256};

use std::backtrace::Backtrace;
use std::path::Path;
use std::sync::Arc;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait IngestService: Send + Sync {
    fn ingest(
        &self,
        dataset_id: &DatasetID,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    fn ingest_from(
        &self,
        dataset_id: &DatasetID,
        fetch: FetchStep,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    fn ingest_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        options: IngestOptions,
        listener: Option<Arc<dyn IngestMultiListener>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)>;
}

#[derive(Debug, Clone)]
pub struct IngestOptions {
    /// Fetch latest data from uncacheable data sources
    pub force_uncacheable: bool,
    /// Pull sources that yield multiple data files until they are
    /// fully exhausted
    pub exhaust_sources: bool,
}

impl Default for IngestOptions {
    fn default() -> Self {
        Self {
            force_uncacheable: false,
            exhaust_sources: false,
        }
    }
}

#[derive(Debug)]
pub enum IngestResult {
    UpToDate {
        uncacheable: bool,
    },
    Updated {
        old_head: Sha3_256,
        new_head: Sha3_256,
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

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }
}

pub struct NullIngestListener;
impl IngestListener for NullIngestListener {}

pub trait IngestMultiListener {
    fn begin_ingest(&self, _dataset_id: &DatasetID) -> Option<Arc<dyn IngestListener>> {
        None
    }
}

pub struct NullIngestMultiListener;
impl IngestMultiListener for NullIngestMultiListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum IngestError {
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
    #[error("Engine error: {0}")]
    EngineError(#[from] EngineError),
    #[error("Pipe command error: {command:?} {source}")]
    PipeError {
        command: Vec<String>,
        source: BoxedError,
        backtrace: Backtrace,
    },
    #[error("Internal error: {source}")]
    InternalError {
        #[from]
        source: BoxedError,
        backtrace: Backtrace,
    },
}

impl IngestError {
    pub fn unreachable<S: AsRef<Path>>(path: S, source: Option<BoxedError>) -> Self {
        IngestError::Unreachable {
            path: path.as_ref().to_str().unwrap().to_owned(),
            source: source,
        }
    }

    pub fn not_found<S: AsRef<Path>>(path: S, source: Option<BoxedError>) -> Self {
        IngestError::NotFound {
            path: path.as_ref().to_str().unwrap().to_owned(),
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

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        IngestError::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
