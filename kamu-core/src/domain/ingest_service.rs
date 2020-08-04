use super::EngineError;
use crate::domain::{DatasetID, DatasetIDBuf};

use std::backtrace::Backtrace;
use std::path::Path;
use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait IngestService {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<IngestResult, IngestError>;

    fn ingest_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<IngestResult, IngestError>)>;
}

#[derive(Debug)]
pub enum IngestResult {
    UpToDate,
    Updated { block_hash: String },
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

pub trait IngestListener: Send {
    fn begin(&mut self) {}
    fn on_stage_progress(&mut self, _stage: IngestStage, _n: u64, _out_of: u64) {}
    fn warn_uncacheable(&mut self) {}
    fn success(&mut self, _result: &IngestResult) {}
    fn error(&mut self, _stage: IngestStage, _error: &IngestError) {}
}

pub struct NullIngestListener;
impl IngestListener for NullIngestListener {}

pub trait IngestMultiListener {
    fn begin_ingest(&mut self, _dataset_id: &DatasetID) -> Option<Arc<Mutex<dyn IngestListener>>> {
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
    #[error("Source is unreachable {path}")]
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

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        IngestError::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
