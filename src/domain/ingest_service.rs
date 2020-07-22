use crate::domain::DatasetID;

use std::backtrace::Backtrace;
use thiserror::Error;

pub trait IngestService {
    fn ingest(
        &mut self,
        dataset_id: &DatasetID,
        listener: Option<&mut dyn IngestListener>,
    ) -> Result<IngestResult, IngestError>;
}

pub enum IngestResult {
    UpToDate,
    Updated,
}

pub trait IngestListener {
    fn warn_uncacheable(&mut self);
    fn block_written(&mut self, hash: String);
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("Fetch stage error")]
    FetchError,
    //#[error("Preparation stage error")]
    //PrepError,
    //#[error("Parse error")]
    //ParseError
    //#[error("Schema error")]
    //SchemaError,
    //#[error("Engine error")]
    //EngineError,
}
