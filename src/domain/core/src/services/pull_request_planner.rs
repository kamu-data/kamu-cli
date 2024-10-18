// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::sync::Arc;

use ::serde::{Deserialize, Serialize};
use internal_error::InternalError;
use opendatafabric::*;
use thiserror::Error;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PullRequestPlanner: Send + Sync {
    // This function descends down the dependency tree of datasets (starting with
    // provided references) assigning depth index to every dataset in the
    // graph(s). Datasets that share the same depth level are independent and
    // can be pulled in parallel.
    async fn collect_pull_graph(
        &self,
        requests: &[PullRequest],
        options: &PullMultiOptions,
        in_multi_tenant_mode: bool,
    ) -> (Vec<PullItem>, Vec<PullResponse>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullItem {
    pub depth: i32,
    pub local_ref: DatasetRef,
    pub remote_ref: Option<DatasetRefRemote>,
    pub original_request: Option<PullRequest>,
}

impl PullItem {
    pub fn into_response_ingest(self, r: PollingIngestResponse) -> PullResponse {
        PullResponse {
            original_request: self.original_request,
            local_ref: Some(r.dataset_ref),
            remote_ref: None,
            result: match r.result {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    pub fn into_response_sync(self, r: SyncResultMulti) -> PullResponse {
        PullResponse {
            original_request: self.original_request,
            local_ref: r.dst.as_local_ref(|_| true).ok(), // TODO: multi-tenancy
            remote_ref: r.src.as_remote_ref(|_| true).ok(),
            result: match r.result {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }

    pub fn into_response_transform(
        self,
        r: (DatasetRef, Result<TransformResult, TransformError>),
    ) -> PullResponse {
        PullResponse {
            original_request: self.original_request,
            local_ref: Some(r.0),
            remote_ref: None,
            result: match r.1 {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

impl PartialOrd for PullItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PullItem {
    fn cmp(&self, other: &Self) -> Ordering {
        let depth_ord = self.depth.cmp(&other.depth);
        if depth_ord != Ordering::Equal {
            return depth_ord;
        }

        if self.remote_ref.is_some() != other.remote_ref.is_some() {
            return if self.remote_ref.is_some() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        match (self.local_ref.alias(), other.local_ref.alias()) {
            (Some(lhs), Some(rhs)) => lhs.cmp(rhs),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            _ => Ordering::Equal,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequest {
    pub local_ref: Option<DatasetRef>,
    pub remote_ref: Option<DatasetRefRemote>,
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
            },
            Err(remote_ref) => Self {
                local_ref: None,
                remote_ref: Some(remote_ref),
            },
        }
    }

    pub fn from_handle(hdl: &DatasetHandle) -> Self {
        Self {
            local_ref: Some(hdl.as_local_ref()),
            remote_ref: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Whether the datasets pulled from remotes should be permanently
    /// associated with them
    pub add_aliases: bool,
    /// Ingest-specific options
    pub ingest_options: PollingIngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
    /// Run compaction of derivative dataset without saving data
    /// if transformation failed due to root dataset compaction
    pub reset_derivatives_on_diverged_input: bool,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            add_aliases: true,
            reset_derivatives_on_diverged_input: false,
            ingest_options: PollingIngestOptions::default(),
            sync_options: SyncOptions::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    pub ingest_options: PollingIngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
    /// Run compaction of all derivative datasets without saving data
    /// if transformation fails due to root dataset compaction
    pub reset_derivatives_on_diverged_input: bool,
}

impl Default for PullMultiOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            all: false,
            add_aliases: true,
            ingest_options: PollingIngestOptions::default(),
            sync_options: SyncOptions::default(),
            reset_derivatives_on_diverged_input: false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait PullListener: Send + Sync {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestListener>>;
    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>>;
    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncListener>>;
}

pub trait PullMultiListener: Send + Sync {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestMultiListener>>;
    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformMultiListener>>;
    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncMultiListener>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PullResult {
    UpToDate(PullResultUpToDate),
    Updated {
        old_head: Option<Multihash>,
        new_head: Multihash,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PullResultUpToDate {
    PollingIngest(PollingInsgestResultUpToDate),
    PushIngest(PushInsgestResultUpToDate),
    Transform,
    Sync,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PollingInsgestResultUpToDate {
    pub uncacheable: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PushInsgestResultUpToDate {
    pub uncacheable: bool,
}

impl From<PollingIngestResult> for PullResult {
    fn from(other: PollingIngestResult) -> Self {
        match other {
            PollingIngestResult::UpToDate { uncacheable, .. } => PullResult::UpToDate(
                PullResultUpToDate::PollingIngest(PollingInsgestResultUpToDate { uncacheable }),
            ),
            PollingIngestResult::Updated {
                old_head, new_head, ..
            } => PullResult::Updated {
                old_head: Some(old_head),
                new_head,
            },
        }
    }
}

impl From<TransformResult> for PullResult {
    fn from(other: TransformResult) -> Self {
        match other {
            TransformResult::UpToDate => PullResult::UpToDate(PullResultUpToDate::Transform),
            TransformResult::Updated { old_head, new_head } => PullResult::Updated {
                old_head: Some(old_head),
                new_head,
            },
        }
    }
}

impl From<SyncResult> for PullResult {
    fn from(other: SyncResult) -> Self {
        match other {
            SyncResult::UpToDate => PullResult::UpToDate(PullResultUpToDate::Sync),
            SyncResult::Updated {
                old_head, new_head, ..
            } => PullResult::Updated { old_head, new_head },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
    PollingIngestError(
        #[from]
        #[backtrace]
        PollingIngestError,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
