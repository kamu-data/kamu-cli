use super::sync_service::*;
use crate::domain::DomainError;
use opendatafabric::{DatasetIDBuf, DatasetRef, DatasetRefBuf, Sha3_256};

use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait PushService: Send + Sync {
    fn push_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = &DatasetRef>,
        options: PushOptions,
        sync_listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<(PushInfo, Result<PushResult, PushError>)>;
}

#[derive(Debug, Clone)]
pub struct PushOptions {
    /// Push all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Push all known datasets
    pub all: bool,
    /// Sync options
    pub sync_options: SyncOptions,
}

impl Default for PushOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            sync_options: SyncOptions::default(),
        }
    }
}

#[derive(Debug)]
pub struct PushInfo {
    pub original_ref: DatasetRefBuf,
    pub local_id: Option<DatasetIDBuf>,
    pub remote_ref: Option<DatasetRefBuf>,
}

#[derive(Debug)]
pub enum PushResult {
    UpToDate,
    Updated {
        old_head: Option<Sha3_256>,
        new_head: Sha3_256,
        num_blocks: usize,
    },
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PushError {
    #[error("No associated push alias")]
    NoTarget,
    #[error("Cannot choose between multiple push aliases")]
    AmbiguousTarget,
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("Sync error: {0}")]
    SyncError(#[from] SyncError),
}
