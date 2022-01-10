// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::ingest_service::*;
use super::sync_service::*;
use super::transform_service::*;
use crate::domain::DomainError;

use chrono::{DateTime, Utc};
use opendatafabric::{
    DatasetName, DatasetRefAny, DatasetRefLocal, DatasetRefRemote, FetchStep, Multihash,
};
use thiserror::Error;

use std::sync::Arc;

///////////////////////////////////////////////////////////////////////////////
// Service
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait PullService: Send + Sync {
    async fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(DatasetRefAny, Result<PullResult, PullError>)>;

    /// A special version of pull that can rename dataset when syncing it from a repository
    async fn sync_from(
        &self,
        remote_ref: &DatasetRefRemote,
        local_name: &DatasetName,
        options: PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResult, PullError>;

    /// A special version of pull that allows overriding the fetch source on root datasets (e.g. to pull data from a specific file)
    async fn ingest_from(
        &self,
        dataset_ref: &DatasetRefLocal,
        fetch: FetchStep,
        options: PullOptions,
        listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<PullResult, PullError>;

    async fn set_watermark(
        &self,
        dataset_ref: &DatasetRefLocal,
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
        old_head: Option<Multihash>,
        new_head: Multihash,
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
