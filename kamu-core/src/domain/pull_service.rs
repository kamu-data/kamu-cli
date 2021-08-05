use super::ingest_service::*;
use super::sync_service::*;
use super::transform_service::*;
use crate::domain::DomainError;
use opendatafabric::{DatasetID, DatasetRef, DatasetRefBuf, Sha3_256};

use chrono::{DateTime, Utc};
use std::sync::{Arc, Mutex};
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

pub trait PullService: Send + Sync {
    fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = &DatasetRef>,
        options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
        sync_listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<(DatasetRefBuf, Result<PullResult, PullError>)>;

    /// A special version of pull that can rename dataset when syncing it from a repository
    fn pull_from(
        &self,
        remote_ref: &DatasetRef,
        local_id: &DatasetID,
        options: PullOptions,
        listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<PullResult, PullError>;

    fn set_watermark(
        &self,
        dataset_id: &DatasetID,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Pull all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Pull all known datasets
    pub all: bool,
    /// Whether the datasets pulled from remotes should be permanently associated with them
    pub create_remote_aliases: bool,
    /// Ingest-specific options
    pub ingest_options: IngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            create_remote_aliases: true,
            ingest_options: IngestOptions::default(),
            sync_options: SyncOptions::default(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum PullResult {
    UpToDate,
    Updated {
        old_head: Option<Sha3_256>,
        new_head: Sha3_256,
        num_blocks: usize,
    },
}

impl From<IngestResult> for PullResult {
    fn from(other: IngestResult) -> Self {
        match other {
            IngestResult::UpToDate { uncacheable: _ } => PullResult::UpToDate,
            IngestResult::Updated {
                old_head,
                new_head,
                num_blocks,
                has_more: _,
                uncacheable: _,
            } => PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks,
            },
        }
    }
}

impl From<TransformResult> for PullResult {
    fn from(other: TransformResult) -> Self {
        match other {
            TransformResult::UpToDate => PullResult::UpToDate,
            TransformResult::Updated {
                old_head,
                new_head,
                num_blocks,
            } => PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks,
            },
        }
    }
}

impl From<SyncResult> for PullResult {
    fn from(other: SyncResult) -> Self {
        match other {
            SyncResult::UpToDate => PullResult::UpToDate,
            SyncResult::Updated {
                old_head,
                new_head,
                num_blocks,
            } => PullResult::Updated {
                old_head,
                new_head,
                num_blocks,
            },
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PullError {
    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("Ingest error: {0}")]
    IngestError(#[from] IngestError),
    #[error("Transform error: {0}")]
    TransformError(#[from] TransformError),
    #[error("Sync error: {0}")]
    SyncError(#[from] SyncError),
}
