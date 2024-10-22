// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
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

    fn prepare_pull_execution_steps(&self, plan: Vec<PullItem>) -> Vec<PullExecutionStep>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullItem {
    pub depth: i32,
    pub local_target: PullItemLocalTarget,
    pub maybe_remote_ref: Option<DatasetRefRemote>,
    pub maybe_original_request: Option<PullRequest>,
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

        if self.maybe_remote_ref.is_some() != other.maybe_remote_ref.is_some() {
            return if self.maybe_remote_ref.is_some() {
                Ordering::Less
            } else {
                Ordering::Greater
            };
        }

        self.local_target.alias().cmp(other.local_target.alias())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullItemLocalTarget {
    Existing(DatasetHandle),
    ToCreate(DatasetAlias),
}

impl PullItemLocalTarget {
    pub fn existing(hdl: DatasetHandle) -> Self {
        Self::Existing(hdl)
    }

    pub fn to_create(alias: DatasetAlias) -> Self {
        Self::ToCreate(alias)
    }

    pub fn alias(&self) -> &DatasetAlias {
        match self {
            Self::Existing(hdl) => &hdl.alias,
            Self::ToCreate(alias) => alias,
        }
    }

    pub fn as_local_ref(&self) -> DatasetRef {
        match self {
            Self::Existing(hdl) => hdl.as_local_ref(),
            Self::ToCreate(alias) => alias.as_local_ref(),
        }
    }

    pub fn as_any_ref(&self) -> DatasetRefAny {
        match self {
            Self::Existing(hdl) => hdl.as_any_ref(),
            Self::ToCreate(alias) => alias.as_any_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullUpdateItem {
    pub depth: i32,
    pub local_handle: DatasetHandle,
    pub maybe_original_request: Option<PullRequest>,
}

impl PullUpdateItem {
    pub fn into_response_ingest(self, r: PollingIngestResponse) -> PullResponse {
        PullResponse {
            original_request: self.maybe_original_request,
            local_ref: Some(r.dataset_ref),
            remote_ref: None,
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
            original_request: self.maybe_original_request,
            local_ref: Some(r.0),
            remote_ref: None,
            result: match r.1 {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullSyncItem {
    pub depth: i32,
    pub local_target: PullItemLocalTarget,
    pub remote_ref: DatasetRefRemote,
    pub maybe_original_request: Option<PullRequest>,
}

impl PullSyncItem {
    pub fn into_response_sync(self, r: SyncResultMulti) -> PullResponse {
        PullResponse {
            original_request: self.maybe_original_request,
            local_ref: r.dst.as_local_ref(|_| true).ok(), // TODO: multi-tenancy
            remote_ref: r.src.as_remote_ref(|_| true).ok(),
            result: match r.result {
                Ok(response) => Ok(response.result.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullExecutionStep {
    pub depth: i32,
    pub details: PullExecutionStepDetails,
}

#[derive(Debug)]
pub enum PullExecutionStepDetails {
    Ingest(Vec<PullUpdateItem>),
    Transform(Vec<PullUpdateItem>),
    Sync(Vec<PullSyncItem>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullRequest {
    Local(DatasetRef),
    Remote(PullRequestRemote),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestRemote {
    pub remote_ref: DatasetRefRemote,
    pub maybe_local_alias: Option<DatasetAlias>,
}

impl PullRequest {
    pub fn local(dataset_ref: DatasetRef) -> Self {
        Self::Local(dataset_ref)
    }

    pub fn remote(remote_ref: DatasetRefRemote, maybe_local_alias: Option<DatasetAlias>) -> Self {
        Self::Remote(PullRequestRemote {
            remote_ref,
            maybe_local_alias,
        })
    }

    pub fn from_any_ref(dataset_ref: &DatasetRefAny, is_repo: impl Fn(&RepoName) -> bool) -> Self {
        // Single-tenant workspace => treat all repo-like references as repos.
        // Multi-tenant workspace => treat all repo-like references as accounts, use
        // repo:// for repos
        match dataset_ref.as_local_ref(is_repo) {
            Ok(local_ref) => Self::local(local_ref),
            Err(remote_ref) => Self::remote(remote_ref, None),
        }
    }

    pub fn local_ref(&self) -> Option<Cow<DatasetRef>> {
        match self {
            PullRequest::Local(local_ref) => Some(Cow::Borrowed(local_ref)),
            PullRequest::Remote(remote) => remote
                .maybe_local_alias
                .as_ref()
                .map(|alias| Cow::Owned(alias.as_local_ref())),
        }
    }

    pub fn remote_ref(&self) -> Option<&DatasetRefRemote> {
        match self {
            PullRequest::Local(_) => None,
            PullRequest::Remote(remote) => Some(&remote.remote_ref),
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
