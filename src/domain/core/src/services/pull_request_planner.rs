// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
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
    async fn build_pull_plan(
        &self,
        request: PullRequest,
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Result<PullPlanIterationJob, PullResponse>;

    // This function descends down the dependency tree of datasets (starting with
    // provided references) assigning depth index to every dataset in the
    // graph(s). Datasets that share the same depth level are independent and
    // can be pulled in parallel.
    async fn build_pull_multi_plan(
        &self,
        requests: &[PullRequest],
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> (Vec<PullPlanIteration>, Vec<PullResponse>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullPlanIteration {
    pub depth: i32,
    // TODO: consider making this a vector of jobs per 1 dataset
    // This would allow finer grain parallelism within same depth regardless of job kind
    // In addition, we would simplify the interface for task system runner
    // Consider removal of *_multi executions in favor of controlling parallelism in use cases
    pub job: PullPlanIterationJob,
}

#[derive(Debug)]
pub enum PullPlanIterationJob {
    Ingest(Vec<PullIngestItem>),
    Transform(Vec<PullTransformItem>),
    Sync((Vec<PullSyncItem>, Vec<SyncRequest>)),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullIngestItem {
    pub depth: i32,
    pub target: ResolvedDataset,
    pub maybe_original_request: Option<PullRequest>,
}

impl PullIngestItem {
    pub fn into_response_ingest(self, r: PollingIngestResponse) -> PullResponse {
        PullResponse {
            maybe_original_request: self.maybe_original_request,
            maybe_local_ref: Some(r.dataset_ref),
            maybe_remote_ref: None,
            result: match r.result {
                Ok(r) => Ok(r.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullTransformItem {
    pub depth: i32,
    pub target: ResolvedDataset,
    pub maybe_original_request: Option<PullRequest>,
    pub plan: TransformPreliminaryPlan,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullSyncItem {
    pub depth: i32,
    pub local_target: PullLocalTarget,
    pub remote_ref: DatasetRefRemote,
    pub maybe_original_request: Option<PullRequest>,
}

impl PullSyncItem {
    pub fn into_response_sync(self, r: SyncResultMulti) -> PullResponse {
        PullResponse {
            maybe_original_request: self.maybe_original_request,
            maybe_local_ref: r.dst.as_local_ref(|_| true).ok(), // TODO: multi-tenancy
            maybe_remote_ref: r.src.as_remote_ref(|_| true).ok(),
            result: match r.result {
                Ok(response) => Ok(response.result.into()),
                Err(e) => Err(e.into()),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullLocalTarget {
    Existing(DatasetHandle),
    ToCreate(DatasetAlias),
}

impl PullLocalTarget {
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

pub trait PullItemCommon {
    fn try_get_handle(&self) -> Option<&DatasetHandle>;
    fn into_original_pull_request(self) -> Option<PullRequest>;
}

impl PullItemCommon for PullIngestItem {
    fn try_get_handle(&self) -> Option<&DatasetHandle> {
        Some(&self.target.handle)
    }

    fn into_original_pull_request(self) -> Option<PullRequest> {
        self.maybe_original_request
    }
}

impl PullItemCommon for PullTransformItem {
    fn try_get_handle(&self) -> Option<&DatasetHandle> {
        Some(&self.target.handle)
    }

    fn into_original_pull_request(self) -> Option<PullRequest> {
        self.maybe_original_request
    }
}

impl PullItemCommon for PullSyncItem {
    fn try_get_handle(&self) -> Option<&DatasetHandle> {
        match &self.local_target {
            PullLocalTarget::Existing(hdl) => Some(hdl),
            PullLocalTarget::ToCreate(_) => None,
        }
    }

    fn into_original_pull_request(self) -> Option<PullRequest> {
        self.maybe_original_request
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullResponse {
    /// Parameters passed into the call. Empty for datasets that were pulled as
    /// recursive dependencies.
    pub maybe_original_request: Option<PullRequest>,
    /// Local dataset handle, if resolved
    pub maybe_local_ref: Option<DatasetRef>,
    /// Destination reference, if resolved
    pub maybe_remote_ref: Option<DatasetRefRemote>,
    /// Result of the push operation
    pub result: Result<PullResult, PullError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct PullOptions {
    /// Pull all dataset dependencies recursively in depth-first order
    pub recursive: bool,
    /// Whether the datasets pulled from remotes should be permanently
    /// associated with them
    pub add_aliases: bool,
    /// Ingest-specific options
    pub ingest_options: PollingIngestOptions,
    /// Sync-specific options,
    pub sync_options: SyncOptions,
    /// Transform-specific options,
    pub transform_options: TransformOptions,
}

impl Default for PullOptions {
    fn default() -> Self {
        Self {
            recursive: false,
            add_aliases: true,
            ingest_options: PollingIngestOptions::default(),
            sync_options: SyncOptions::default(),
            transform_options: TransformOptions::default(),
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
    PollingIngest(PollingIngestResultUpToDate),
    PushIngest(PushInsgestResultUpToDate),
    Transform,
    Sync,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PollingIngestResultUpToDate {
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
                PullResultUpToDate::PollingIngest(PollingIngestResultUpToDate { uncacheable }),
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
