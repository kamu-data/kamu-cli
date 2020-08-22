use super::ingest_service::*;
use super::transform_service::*;
use crate::domain::{DatasetID, DatasetIDBuf};

use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait PullService {
    fn pull_multi(
        &mut self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        recursive: bool,
        all: bool,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)>;
}

#[derive(Debug)]
pub enum PullResult {
    UpToDate,
    Updated { block_hash: String },
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PullError {
    #[error("Ingest error: {0}")]
    IngestError(#[from] IngestError),
    #[error("Transform error: {0}")]
    TransformError(#[from] TransformError),
}
