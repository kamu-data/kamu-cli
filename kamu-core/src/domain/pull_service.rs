use super::ingest_service::*;
use super::transform_service::*;
use crate::domain::DomainError;
use opendatafabric::{DatasetID, DatasetIDBuf, Sha3_256};

use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait PullService: Send + Sync {
    fn pull_multi(
        &self,
        dataset_ids: &mut dyn Iterator<Item = &DatasetID>,
        options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)>;

    fn set_watermark(
        &self,
        dataset_id: &DatasetID,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError>;
}

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Pull all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Pull all known datasets
    pub all: bool,
    /// Ingest-specific options
    pub ingest_options: IngestOptions,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            ingest_options: IngestOptions::default(),
        }
    }
}

#[derive(Debug)]
pub enum PullResult {
    UpToDate,
    Updated { block_hash: Sha3_256 },
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PullError {
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("Ingest error: {0}")]
    IngestError(#[from] IngestError),
    #[error("Transform error: {0}")]
    TransformError(#[from] TransformError),
}
