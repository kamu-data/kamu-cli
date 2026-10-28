// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::ResolvedDataset;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The transactional steps of updating one local dataset, shared by
/// `kamu pull` and the update task. Each step opens a short transaction of
/// its own and returns what it read detached from it, so no transaction is
/// held while the caller ingests, transforms or transfers between steps.
#[async_trait::async_trait]
pub trait DatasetUpdateSteps: Send + Sync {
    /// Plans the first ingest iteration and resolves the dataset's
    /// environment variables. `None` when the dataset was deleted
    async fn plan_ingest(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<PlannedIngest>, PollingIngestPlanError>;

    /// Elaborates the dataset's transform. `None` when the dataset was deleted
    async fn elaborate_transform(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<ElaboratedTransform>, TransformElaborateError>;

    /// Reads the destination of a pull with the `HEAD` its commit will
    /// expect. `None` when an existing dataset was deleted
    async fn resolve_sync_target(
        &self,
        target: &PullLocalTarget,
    ) -> Result<Option<LocalSyncTarget>, SyncError>;

    /// Moves `HEAD` from `old_head` to `new_head` if it is still `old_head`
    async fn commit_head(
        &self,
        target: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<HeadCommit, InternalError>;

    /// Transfers with no transaction held, then commits. A dataset the pull
    /// creates gets `maybe_pull_alias` in the commit's transaction, so it
    /// never exists without it
    async fn transfer_and_commit_sync(
        &self,
        request: LocalSyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<SyncResult, SyncError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The first iteration of an ingest, with what it runs on
#[derive(Debug)]
pub struct PlannedIngest {
    pub target: ResolvedDataset,
    pub plan: PollingIngestPlan,
    pub env_vars: IngestEnvVars,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A transform elaborated on the dataset as it is now
#[derive(Debug)]
pub struct ElaboratedTransform {
    pub target: ResolvedDataset,
    pub elaboration: TransformElaboration,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How moving `HEAD` ended
#[derive(Debug, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum HeadCommit {
    Committed,
    /// Another writer moved `HEAD` first
    HeadMoved(odf::dataset::RefCASError),
    DatasetDeleted,
    /// The current account may not move `HEAD`
    NotAllowed(odf::AccessError),
}

impl HeadCommit {
    /// The outcome as a reference update result, for callers that report
    /// every failed commit as an error
    pub fn into_set_ref_result(
        result: Result<Self, InternalError>,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        match result? {
            Self::Committed => Ok(()),
            Self::HeadMoved(e) => Err(odf::dataset::SetChainRefError::CASFailed(e)),
            Self::NotAllowed(e) => Err(odf::dataset::SetChainRefError::Access(e)),
            Self::DatasetDeleted => {
                Err("The dataset was deleted during the update".int_err().into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
