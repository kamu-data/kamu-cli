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

    async fn build_pull_plan_all_owner_datasets(
        &self,
        options: &PullOptions,
        tenancy_config: TenancyConfig,
    ) -> Result<(Vec<PullPlanIteration>, Vec<PullResponse>), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullPlanIteration {
    pub depth: i32,
    pub jobs: Vec<PullPlanIterationJob>,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum PullPlanIterationJob {
    Ingest(PullIngestItem),
    Transform(PullTransformItem),
    Sync(PullSyncItem),
}

impl PullPlanIterationJob {
    pub fn as_common_item(&self) -> &dyn PullItemCommon {
        match self {
            Self::Ingest(pii) => pii,
            Self::Transform(pti) => pti,
            Self::Sync(psi) => psi,
        }
    }

    pub fn into_original_pull_request(self) -> Option<PullRequest> {
        match self {
            Self::Ingest(pii) => pii.maybe_original_request,
            Self::Transform(pti) => pti.maybe_original_request,
            Self::Sync(psi) => psi.maybe_original_request,
        }
    }

    pub fn detach_from_transaction(&self) {
        match self {
            Self::Ingest(pii) => pii.target.detach_from_transaction(),
            Self::Transform(pti) => {
                pti.target.detach_from_transaction();
                pti.plan.datasets_map.detach_from_transaction();
            }
            Self::Sync(psi) => {
                psi.sync_request.src.detach_from_transaction();
                psi.sync_request.dst.detach_from_transaction();
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullIngestItem {
    pub depth: i32,
    pub target: ResolvedDataset,
    pub metadata_state: Box<DataWriterMetadataState>,
    pub maybe_original_request: Option<PullRequest>,
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
    pub remote_ref: odf::DatasetRefRemote,
    pub maybe_original_request: Option<PullRequest>,
    pub sync_request: Box<SyncRequest>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullLocalTarget {
    Existing(odf::DatasetHandle),
    ToCreate(odf::DatasetAlias),
}

impl PullLocalTarget {
    pub fn existing(hdl: odf::DatasetHandle) -> Self {
        Self::Existing(hdl)
    }

    pub fn to_create(alias: odf::DatasetAlias) -> Self {
        Self::ToCreate(alias)
    }

    pub fn alias(&self) -> &odf::DatasetAlias {
        match self {
            Self::Existing(hdl) => &hdl.alias,
            Self::ToCreate(alias) => alias,
        }
    }

    pub fn as_local_ref(&self) -> odf::DatasetRef {
        match self {
            Self::Existing(hdl) => hdl.as_local_ref(),
            Self::ToCreate(alias) => alias.as_local_ref(),
        }
    }

    pub fn as_any_ref(&self) -> odf::DatasetRefAny {
        match self {
            Self::Existing(hdl) => hdl.as_any_ref(),
            Self::ToCreate(alias) => alias.as_any_ref(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PullRequest {
    Local(odf::DatasetRef),
    Remote(PullRequestRemote),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PullRequestRemote {
    pub remote_ref: odf::DatasetRefRemote,
    pub maybe_local_alias: Option<odf::DatasetAlias>,
}

impl PullRequest {
    pub fn local(dataset_ref: odf::DatasetRef) -> Self {
        Self::Local(dataset_ref)
    }

    pub fn remote(
        remote_ref: odf::DatasetRefRemote,
        maybe_local_alias: Option<odf::DatasetAlias>,
    ) -> Self {
        Self::Remote(PullRequestRemote {
            remote_ref,
            maybe_local_alias,
        })
    }

    pub fn from_any_ref(
        dataset_ref: &odf::DatasetRefAny,
        is_repo: impl Fn(&odf::RepoName) -> bool,
    ) -> Self {
        // Single-tenant workspace => treat all repo-like references as repos.
        // Multi-tenant workspace => treat all repo-like references as accounts, use
        // repo:// for repos
        match dataset_ref.as_local_ref(is_repo) {
            Ok(local_ref) => Self::local(local_ref),
            Err(remote_ref) => Self::remote(remote_ref, None),
        }
    }

    pub fn local_ref(&self) -> Option<Cow<odf::DatasetRef>> {
        match self {
            PullRequest::Local(local_ref) => Some(Cow::Borrowed(local_ref)),
            PullRequest::Remote(remote) => remote
                .maybe_local_alias
                .as_ref()
                .map(|alias| Cow::Owned(alias.as_local_ref())),
        }
    }

    pub fn remote_ref(&self) -> Option<&odf::DatasetRefRemote> {
        match self {
            PullRequest::Local(_) => None,
            PullRequest::Remote(remote) => Some(&remote.remote_ref),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait PullItemCommon {
    fn try_get_written_handle(&self) -> Option<&odf::DatasetHandle>;
    fn get_read_handles(&self) -> Vec<&odf::DatasetHandle>;
}

impl PullItemCommon for PullIngestItem {
    fn try_get_written_handle(&self) -> Option<&odf::DatasetHandle> {
        Some(self.target.get_handle())
    }

    fn get_read_handles(&self) -> Vec<&odf::DatasetHandle> {
        vec![]
    }
}

impl PullItemCommon for PullTransformItem {
    fn try_get_written_handle(&self) -> Option<&odf::DatasetHandle> {
        Some(self.target.get_handle())
    }

    fn get_read_handles(&self) -> Vec<&odf::DatasetHandle> {
        let mut read_handles = Vec::new();
        for hdl in self.plan.datasets_map.iterate_all_handles() {
            if hdl != self.target.get_handle() {
                read_handles.push(hdl);
            }
        }
        read_handles
    }
}

impl PullItemCommon for PullSyncItem {
    fn try_get_written_handle(&self) -> Option<&odf::DatasetHandle> {
        match &self.local_target {
            PullLocalTarget::Existing(hdl) => Some(hdl),
            PullLocalTarget::ToCreate(_) => None,
        }
    }

    fn get_read_handles(&self) -> Vec<&odf::DatasetHandle> {
        vec![]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct PullResponse {
    /// Parameters passed into the call. Empty for datasets that were pulled as
    /// recursive dependencies.
    pub maybe_original_request: Option<PullRequest>,
    /// Local dataset handle, if resolved
    pub maybe_local_ref: Option<odf::DatasetRef>,
    /// Destination reference, if resolved
    pub maybe_remote_ref: Option<odf::DatasetRefRemote>,
    /// Result of the pull operation
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
        old_head: Option<odf::Multihash>,
        new_head: odf::Multihash,
    },
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum PullResultUpToDate {
    PollingIngest(PollingIngestResultUpToDate),
    PushIngest(PushIngestResultUpToDate),
    Transform,
    Sync,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PollingIngestResultUpToDate {
    pub uncacheable: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct PushIngestResultUpToDate {
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
        odf::DatasetNotFoundError,
    ),

    #[error("Cannot choose between multiple pull aliases")]
    AmbiguousSource,

    #[error("The dataset was previously saved via '{0}' alias")]
    SaveUnderDifferentAlias(String),

    #[error("{0}")]
    InvalidOperation(String),

    #[error(transparent)]
    ScanMetadata(
        #[from]
        #[backtrace]
        ScanMetadataError,
    ),

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
    Access(
        #[from]
        #[backtrace]
        odf::metadata::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
