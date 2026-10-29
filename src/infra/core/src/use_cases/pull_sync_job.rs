// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one sync job of `kamu pull`: reads the target, transfers and commits
pub(crate) struct PullSyncJob {
    update_steps: Arc<dyn DatasetUpdateSteps>,
}

impl PullSyncJob {
    pub(crate) fn new(update_steps: Arc<dyn DatasetUpdateSteps>) -> Self {
        Self { update_steps }
    }

    pub(crate) async fn run(
        self,
        psi: PullSyncItem,
        options: PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResponse, InternalError> {
        let result = match self
            .update_steps
            .resolve_sync_target(&psi.local_target)
            .await
        {
            Ok(Some(dst)) => {
                let sync_result = self
                    .transfer_and_commit(&psi, dst, &options, listener.clone())
                    .await;
                Self::notify(listener.as_deref(), sync_result.as_ref());
                sync_result.map(Into::into).map_err(Self::into_pull_error)
            }
            Ok(None) => {
                let local_ref = psi.local_target.as_local_ref();
                Self::notify(
                    listener.as_deref(),
                    Err(&DatasetAnyRefUnresolvedError::new(local_ref.clone()).into()),
                );
                Err(PullError::NotFound(odf::DatasetNotFoundError::new(
                    local_ref,
                )))
            }
            Err(e) => {
                Self::notify(listener.as_deref(), Err(&e));
                Err(Self::into_pull_error(e))
            }
        };

        Ok(PullResponse {
            maybe_original_request: psi.maybe_original_request,
            maybe_local_ref: Some(psi.local_target.as_local_ref()), // TODO: multi-tenancy
            maybe_remote_ref: Some(psi.remote_ref),
            result,
        })
    }

    /// The listener hears the outcome after the commit, which decides it
    fn notify(listener: Option<&dyn SyncListener>, sync_result: Result<&SyncResult, &SyncError>) {
        if let Some(listener) = listener {
            match sync_result {
                Ok(result) => listener.success(result),
                Err(e) => listener.error(e),
            }
        }
    }

    /// A lost commit race is the pull's [`PullError::HeadMoved`], the same as
    /// for an ingest or a transform. A local commit always knows the heads.
    fn into_pull_error(error: SyncError) -> PullError {
        if let SyncError::HeadMoved(SyncHeadMovedError { heads: Some(e) }) = error {
            PullError::HeadMoved(e)
        } else {
            error.into()
        }
    }

    async fn transfer_and_commit(
        &self,
        psi: &PullSyncItem,
        dst: LocalSyncTarget,
        options: &PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncError> {
        self.update_steps
            .transfer_and_commit_sync(
                LocalSyncRequest {
                    src: psi.remote_ref.clone(),
                    dst,
                },
                options.sync_options,
                listener,
                options.add_aliases.then_some(&psi.remote_ref),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
