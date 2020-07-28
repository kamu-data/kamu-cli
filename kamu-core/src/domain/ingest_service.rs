use crate::domain::{DatasetID, DatasetIDBuf};

use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait IngestService {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        listener: Option<Box<dyn IngestListener>>,
    ) -> Result<IngestResult, IngestError>;

    fn ingest_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        listener: Option<&mut dyn IngestMultiListener>,
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
    fn on_stage_progress(&mut self, _stage: IngestStage, _n: usize, _out_of: usize) {}
    fn warn_uncacheable(&mut self) {}
    fn success(&mut self, _result: &IngestResult) {}
    fn error(&mut self, _stage: IngestStage, _error: &IngestError) {}
}

pub struct NullIngestListener;
impl IngestListener for NullIngestListener {}

pub trait IngestMultiListener {
    fn begin_ingest(&mut self, _dataset_id: &DatasetID) -> Option<Box<dyn IngestListener>> {
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
    FetchError,
    //#[error("Preparation stage error")]
    //PrepareError,
    //#[error("Read error")]
    //ReadError
    //#[error("Schema error")]
    //SchemaError,
    //#[error("Engine error")]
    //EngineError,
}
