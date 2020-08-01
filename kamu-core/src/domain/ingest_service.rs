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

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("Fetch stage error")]
    FetchError(#[from] FetchError),
    #[error("Preparation stage error")]
    PrepError(#[from] PrepError),
    //#[error("Read error")]
    //ReadError
    //#[error("Schema error")]
    //SchemaError,
    //#[error("Engine error")]
    //EngineError,
}

#[derive(Error, Debug)]
pub enum FetchError {
    #[error("Source not found at {path}")]
    NotFound { path: String, backtrace: Backtrace },
    #[error("{0}")]
    InternalError(#[from] Box<dyn std::error::Error + Send>),
}

impl FetchError {
    pub fn internal(e: impl std::error::Error + 'static + Send) -> Self {
        FetchError::InternalError(Box::new(e))
    }

    pub fn not_found<S: AsRef<Path>>(path: S) -> Self {
        FetchError::NotFound {
            path: path.as_ref().to_str().unwrap().to_owned(),
            backtrace: Backtrace::capture(),
        }
    }
}

#[derive(Error, Debug)]
pub enum PrepError {
    #[error("{0}")]
    InternalError(#[from] Box<dyn std::error::Error + Send>),
}

impl PrepError {
    pub fn internal(e: impl std::error::Error + 'static + Send) -> Self {
        PrepError::InternalError(Box::new(e))
    }
}
