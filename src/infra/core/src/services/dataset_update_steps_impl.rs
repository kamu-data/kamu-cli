// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::{transactional_method1, transactional_method2, transactional_method3};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetHeadCommitter, DatasetRegistry, DatasetRegistryExt};

use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetUpdateSteps)]
pub struct DatasetUpdateStepsImpl {
    catalog: Catalog,
    // Resolved only by a sync, so callers that never sync need no sync service
    sync_service: Lazy<Arc<dyn SyncService>>,
}

impl DatasetUpdateStepsImpl {
    #[transactional_method1(local_sync_committer: Arc<dyn LocalSyncCommitter>)]
    async fn commit_sync(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError> {
        local_sync_committer.commit(transferred).await
    }

    /// Commits, and stores the pull alias of a dataset the commit created in
    /// the same transaction
    #[transactional_method3(local_sync_committer: Arc<dyn LocalSyncCommitter>, dataset_registry: Arc<dyn DatasetRegistry>, remote_alias_registry: Arc<dyn RemoteAliasesRegistry>)]
    async fn commit_sync_with_pull_alias(
        &self,
        transferred: TransferredSync,
        pull_alias: &odf::DatasetRefRemote,
    ) -> Result<SyncResult, SyncError> {
        let local_ref = match &transferred.target {
            TransferredSyncTarget::Existing { dataset, .. } => dataset.get_handle().as_local_ref(),
            TransferredSyncTarget::New { alias, .. } => alias.as_local_ref(),
        };

        let sync_result = local_sync_committer.commit(transferred).await?;

        if let SyncResult::Updated { old_head: None, .. } = &sync_result {
            let handle = dataset_registry
                .resolve_dataset_handle_by_ref(&local_ref)
                .await
                .int_err()?;
            remote_alias_registry
                .get_remote_aliases(&handle)
                .await?
                .add(pull_alias, RemoteAliasKind::Pull)
                .await?;
        }

        Ok(sync_result)
    }

    async fn commit_transferred(
        &self,
        transferred: TransferredSync,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        let result = match maybe_pull_alias {
            Some(pull_alias) => {
                self.commit_sync_with_pull_alias(transferred, pull_alias)
                    .await
            }
            None => self.commit_sync(transferred).await,
        };
        match result {
            Ok(sync_result) => Ok(Some(sync_result)),
            // The committer reopens an existing target by its ID, so a deleted one is not found
            Err(SyncError::DatasetNotFound(_)) => Ok(None),
            Err(
                e @ (SyncError::RefCollision(_)
                | SyncError::NameCollision(_)
                | SyncError::CreateDatasetFailed(_)
                | SyncError::UnsupportedProtocol(_)
                | SyncError::UnsupportedIpfsStorageType(_)
                | SyncError::RemoteByIdUnsupported(_)
                | SyncError::InvalidDestination(_)
                | SyncError::RepositoryNotFound(_)
                | SyncError::DatasetsDiverged(_)
                | SyncError::DatasetIdMismatch(_)
                | SyncError::DestinationAhead(_)
                | SyncError::OverwriteSeedBlock(_)
                | SyncError::Corrupted(_)
                | SyncError::HeadMoved(_)
                | SyncError::Access(_)
                | SyncError::Internal(_)),
            ) => Err(e),
        }
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, polling_ingest_planner: Arc<dyn PollingIngestPlanner>)]
    async fn plan_ingest_transactionally(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<PlannedIngest>, PollingIngestPlanError> {
        let Some(dataset) = dataset_registry
            .try_get_dataset_by_ref(&target.id.as_local_ref())
            .await?
        else {
            return Ok(None);
        };

        let plan = polling_ingest_planner.plan_ingest(dataset.clone()).await?;
        let env_vars = polling_ingest_planner.resolve_env_vars(&target.id).await?;

        dataset.detach_from_transaction();

        Ok(Some(PlannedIngest {
            target: dataset,
            plan,
            env_vars,
        }))
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, transform_planner: Arc<dyn TransformPlanner>)]
    async fn elaborate_transform_transactionally(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<ElaboratedTransform>, TransformElaborateError> {
        let Some(dataset) = dataset_registry
            .try_get_dataset_by_ref(&target.id.as_local_ref())
            .await?
        else {
            return Ok(None);
        };

        let elaboration = transform_planner
            .elaborate_transform(dataset.clone())
            .await?;

        dataset.detach_from_transaction();
        if let TransformElaboration::Elaborated(plan) = &elaboration {
            plan.datasets_map.detach_from_transaction();
        }

        Ok(Some(ElaboratedTransform {
            target: dataset,
            elaboration,
        }))
    }

    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn resolve_sync_target_transactionally(
        &self,
        target: &PullLocalTarget,
    ) -> Result<Option<LocalSyncTarget>, SyncError> {
        let sync_target = target
            .resolve_sync_target(dataset_registry.as_ref())
            .await?;
        if let Some(sync_target) = &sync_target {
            sync_target.detach_from_transaction();
        }
        Ok(sync_target)
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_head_transactionally(
        &self,
        target: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<HeadCommit, InternalError> {
        let Some(dataset) = dataset_registry
            .try_get_dataset_by_ref(&target.id.as_local_ref())
            .await?
        else {
            return Ok(HeadCommit::DatasetDeleted);
        };

        match dataset_head_committer
            .commit_head(&dataset, Some(old_head), new_head)
            .await
        {
            Ok(()) => Ok(HeadCommit::Committed),
            Err(odf::dataset::SetChainRefError::CASFailed(e)) => Ok(HeadCommit::HeadMoved(e)),
            Err(odf::dataset::SetChainRefError::Access(e)) => Ok(HeadCommit::NotAllowed(e)),
            Err(
                e @ (odf::dataset::SetChainRefError::BlockNotFound(_)
                | odf::dataset::SetChainRefError::Internal(_)),
            ) => Err(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Each step is an info span named after it, with the dataset and how the
// step ended in `outcome`
#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl DatasetUpdateSteps for DatasetUpdateStepsImpl {
    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_plan_ingest, skip_all, fields(%target, outcome))]
    async fn plan_ingest(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<PlannedIngest>, PollingIngestPlanError> {
        let result = self.plan_ingest_transactionally(target).await;
        match &result {
            Ok(Some(_)) => SpanOutcome::Planned,
            Ok(None) => SpanOutcome::DatasetDeleted,
            Err(_) => SpanOutcome::Failed,
        }
        .record();
        result
    }

    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_elaborate_transform, skip_all, fields(%target, outcome))]
    async fn elaborate_transform(
        &self,
        target: &odf::DatasetHandle,
    ) -> Result<Option<ElaboratedTransform>, TransformElaborateError> {
        let result = self.elaborate_transform_transactionally(target).await;
        match &result {
            Ok(Some(ElaboratedTransform {
                elaboration: TransformElaboration::Elaborated(_),
                ..
            })) => SpanOutcome::Elaborated,
            Ok(Some(ElaboratedTransform {
                elaboration: TransformElaboration::UpToDate,
                ..
            })) => SpanOutcome::UpToDate,
            Ok(None) => SpanOutcome::DatasetDeleted,
            Err(_) => SpanOutcome::Failed,
        }
        .record();
        result
    }

    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_resolve_sync_target, skip_all, fields(?target, outcome))]
    async fn resolve_sync_target(
        &self,
        target: &PullLocalTarget,
    ) -> Result<Option<LocalSyncTarget>, SyncError> {
        let result = self.resolve_sync_target_transactionally(target).await;
        match &result {
            Ok(Some(LocalSyncTarget::Existing { .. })) => SpanOutcome::Existing,
            Ok(Some(LocalSyncTarget::New(_))) => SpanOutcome::New,
            Ok(None) => SpanOutcome::DatasetDeleted,
            Err(e) => SpanOutcome::from(e),
        }
        .record();
        result
    }

    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_commit_head, skip_all, fields(%target, %new_head, outcome))]
    async fn commit_head(
        &self,
        target: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<HeadCommit, InternalError> {
        let result = self
            .commit_head_transactionally(target, old_head, new_head)
            .await;
        match &result {
            Ok(head_commit) => SpanOutcome::from(head_commit),
            Err(_) => SpanOutcome::Failed,
        }
        .record();
        result
    }

    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_transfer_and_commit_sync, skip_all, fields(?request, outcome))]
    async fn transfer_and_commit_sync(
        &self,
        request: LocalSyncRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        let sync_service = self.sync_service.get().int_err()?;
        let result = match sync_service
            .transfer_into_local(request, options, listener)
            .await
        {
            Ok(SyncTransfer::UpToDate) => Ok(Some(SyncResult::UpToDate)),
            Ok(SyncTransfer::Transferred(transferred)) => {
                self.commit_transferred(transferred, maybe_pull_alias).await
            }
            Err(e) => Err(e),
        };
        match &result {
            Ok(Some(SyncResult::Updated { .. })) => SpanOutcome::Updated,
            Ok(Some(SyncResult::UpToDate)) => SpanOutcome::UpToDate,
            Ok(None) => SpanOutcome::DatasetDeleted,
            Err(e) => SpanOutcome::from(e),
        }
        .record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
