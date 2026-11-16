// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::DatasetRegistry;

use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one sync job of `kamu pull`: reads the target, transfers and commits,
/// then records the pull alias of a dataset the sync created
pub(crate) struct PullSyncJob {
    update_steps: Arc<dyn DatasetUpdateSteps>,
    catalog: dill::Catalog,
}

#[common_macros::method_names_consts]
impl PullSyncJob {
    pub(crate) fn new(update_steps: Arc<dyn DatasetUpdateSteps>, catalog: dill::Catalog) -> Self {
        Self {
            update_steps,
            catalog,
        }
    }

    #[tracing::instrument(level = "info", name = PullSyncJob_run, skip_all, fields(target = %psi.local_target.as_local_ref(), outcome))]
    pub(crate) async fn run(
        self,
        psi: PullSyncItem,
        options: PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResponse, InternalError> {
        let response = self.run_job(psi, options, listener).await;
        SpanOutcome::of_pull_response(&response).record();
        response
    }

    async fn run_job(
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
            Ok(Some(dst)) => match self
                .transfer_and_commit(&psi, dst, &options, listener.clone())
                .await
            {
                Ok(Some(sync_result)) => {
                    Self::notify(listener.as_deref(), Ok(&sync_result));
                    Ok(sync_result.into())
                }
                Ok(None) => Err(Self::dataset_deleted(&psi, listener.as_deref())),
                Err(e) => {
                    Self::notify(listener.as_deref(), Err(&e));
                    Err(Self::into_pull_error(e))
                }
            },
            Ok(None) => Err(Self::dataset_deleted(&psi, listener.as_deref())),
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

    /// Reports the outcome to the listener; a transfer is reported only after
    /// its commit, which decides the outcome
    fn notify(listener: Option<&dyn SyncListener>, sync_result: Result<&SyncResult, &SyncError>) {
        if let Some(listener) = listener {
            match sync_result {
                Ok(result) => listener.success(result),
                Err(e) => listener.error(e),
            }
        }
    }

    fn dataset_deleted(psi: &PullSyncItem, listener: Option<&dyn SyncListener>) -> PullError {
        let local_ref = psi.local_target.as_local_ref();
        Self::notify(
            listener,
            Err(&DatasetAnyRefUnresolvedError::new(local_ref.clone()).into()),
        );
        PullError::NotFound(odf::DatasetNotFoundError::new(local_ref))
    }

    /// A lost commit race is the pull's [`PullError::RefMoved`], the same as
    /// for an ingest or a transform. A local commit always knows the block hashes.
    fn into_pull_error(error: SyncError) -> PullError {
        if let SyncError::RefMoved(SyncRefMovedError {
            reported: Some(e), ..
        }) = error
        {
            PullError::RefMoved(e)
        } else {
            error.into()
        }
    }

    async fn transfer_and_commit(
        &self,
        psi: &PullSyncItem,
        dst: SyncFromTarget,
        options: &PullOptions,
        listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<Option<SyncResult>, SyncError> {
        let sync_result = self
            .update_steps
            .transfer_and_commit_sync(
                SyncFromRequest {
                    src: psi.remote_ref.clone(),
                    dst,
                },
                options.sync_options,
                listener,
            )
            .await?;

        if options.add_aliases
            && let Some(SyncResult::Updated { old_head: None, .. }) = &sync_result
        {
            self.store_pull_alias(&psi.local_target.as_local_ref(), &psi.remote_ref)
                .await?;
        }

        Ok(sync_result)
    }

    /// Records `remote_ref` as the pull alias of the dataset the sync created
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, remote_alias_registry: Arc<dyn RemoteAliasesRegistry>)]
    async fn store_pull_alias(
        &self,
        local_ref: &odf::DatasetRef,
        remote_ref: &odf::DatasetRefRemote,
    ) -> Result<(), SyncError> {
        let dataset_handle = dataset_registry
            .resolve_dataset_handle_by_ref(local_ref)
            .await
            .int_err()?;
        remote_alias_registry
            .get_remote_aliases(&dataset_handle)
            .await?
            .add(remote_ref, RemoteAliasKind::Pull)
            .await?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
