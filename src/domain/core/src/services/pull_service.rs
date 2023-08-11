// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use opendatafabric::*;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;
use crate::*;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PullService: Send + Sync {
    async fn pull(
        &self,
        dataset_ref: &DatasetRefAny,
        options: PullOptions,
        listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResult, PullError>;

    async fn pull_ext(
        &self,
        request: &PullRequest,
        options: PullOptions,
        listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResult, PullError>;

    async fn pull_multi(
        &self,
        dataset_refs: Vec<DatasetRefAny>,
        options: PullMultiOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError>;

    async fn pull_multi_ext(
        &self,
        requests: Vec<PullRequest>,
        options: PullMultiOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError>;

    async fn set_watermark(
        &self,
        dataset_ref: &DatasetRef,
        watermark: DateTime<Utc>,
    ) -> Result<PullResult, SetWatermarkError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct PullRequest {
    pub local_ref: Option<DatasetRef>,
    pub remote_ref: Option<DatasetRefRemote>,
    /// Allows to override the fetch source on root datasets (e.g. to pull data
    /// from a specific file)
    pub ingest_from: Option<FetchStep>,
}

impl PullRequest {
    pub fn from_any_ref(dataset_ref: &DatasetRefAny, is_repo: impl Fn(&RepoName) -> bool) -> Self {
        // Single-tenant workspace => treat all repo-like references as repos.
        // Multi-tenant workspace => treat all repo-like references as accounts, use
        // repo:// for repos
        match dataset_ref.as_local_ref(is_repo) {
            Ok(local_ref) => Self {
                local_ref: Some(local_ref),
                remote_ref: None,
                ingest_from: None,
            },
            Err(remote_ref) => Self {
                local_ref: None,
                remote_ref: Some(remote_ref),
                ingest_from: None,
            },
        }
    }
}

#[derive(Debug)]
pub struct PullResponse {
    /// Parameters passed into the call. Empty for datasets that were pulled as
    /// recursive dependencies.
    pub original_request: Option<PullRequest>,
    /// Local dataset handle, if resolved
    pub local_ref: Option<DatasetRef>,
    /// Destination reference, if resolved
    pub remote_ref: Option<DatasetRefRemote>,
    /// Result of the push operation
    pub result: Result<PullResult, PullError>,
}

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Whether the datasets pulled from remotes should be permanently
    /// associated with them
    pub add_aliases: bool,
    /// Ingest-specific options
    pub ingest_options: IngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            add_aliases: true,
            ingest_options: IngestOptions::default(),
            sync_options: SyncOptions::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PullMultiOptions {
    /// Pull all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Pull all known datasets
    pub all: bool,
    /// Whether the datasets pulled from remotes should be permanently
    /// associated with them
    pub add_aliases: bool,
    /// Ingest-specific options
    pub ingest_options: IngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
}

impl Default for PullMultiOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            add_aliases: true,
            ingest_options: IngestOptions::default(),
            sync_options: SyncOptions::default(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

pub trait PullListener: Send + Sync {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn IngestListener>>;
    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>>;
    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncListener>>;
}

pub trait PullMultiListener: Send + Sync {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn IngestMultiListener>>;
    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformMultiListener>>;
    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncMultiListener>>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum PullResult {
    UpToDate,
    Updated {
        old_head: Option<Multihash>,
        new_head: Multihash,
        num_blocks: usize,
    },
}

impl From<IngestResult> for PullResult {
    fn from(other: IngestResult) -> Self {
        match other {
            IngestResult::UpToDate {
                no_polling_source: _,
                uncacheable: _,
            } => PullResult::UpToDate,
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
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error("Source is not specified and there is no associated pull alias")]
    NoSource,
    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,
    #[error("{0}")]
    InvalidOperation(String),
    #[error(transparent)]
    IngestError(
        #[from]
        #[backtrace]
        IngestError,
    ),
    #[error(transparent)]
    TransformError(
        #[from]
        #[backtrace]
        TransformError,
    ),
    #[error(transparent)]
    SyncError(
        #[from]
        #[backtrace]
        SyncError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SetWatermarkError {
    #[error(transparent)]
    NotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),

    #[error("Attempting to set watermark on a remote dataset")]
    IsRemote,

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<GetDatasetError> for SetWatermarkError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::NotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<DatasetActionUnauthorizedError> for SetWatermarkError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
