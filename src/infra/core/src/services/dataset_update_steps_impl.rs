// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::{transactional_method1, transactional_method2};
use dill::*;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{
    CreateDatasetUseCase,
    CreateDatasetUseCaseOptions,
    DatasetHeadCommitter,
    DatasetRegistry,
    DatasetRegistryExt,
    ResolvedDataset,
};

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
    /// Moves `HEAD` of an existing dataset from the head the transfer started
    /// from
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_sync_into_existing(
        &self,
        dataset: &ResolvedDataset,
        expected_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), SyncError> {
        // The transfer ran detached; the commit needs this transaction, and
        // the dataset may have been deleted meanwhile
        let dataset = dataset_registry.get_dataset_by_id(dataset.get_id()).await?;
        dataset_head_committer
            .commit_head(&dataset, expected_head, new_head)
            .await?;
        Ok(())
    }

    /// Creates the dataset with `HEAD` at the transferred head, and stores its
    /// pull alias in the same transaction
    #[transactional_method2(create_dataset_use_case: Arc<dyn CreateDatasetUseCase>, remote_alias_registry: Arc<dyn RemoteAliasesRegistry>)]
    async fn commit_sync_into_new(
        &self,
        alias: &odf::DatasetAlias,
        seed_block: odf::MetadataBlockTyped<odf::metadata::Seed>,
        options: CreateDatasetUseCaseOptions,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<(), SyncError> {
        let created = create_dataset_use_case
            .execute(alias, seed_block, options)
            .await?;

        if let Some(pull_alias) = maybe_pull_alias {
            remote_alias_registry
                .get_remote_aliases(&created.dataset_handle)
                .await?
                .add(pull_alias, RemoteAliasKind::Pull)
                .await?;
        }
        Ok(())
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
    ) -> Result<Option<SyncFromTarget>, SyncError> {
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
    ) -> Result<Option<SyncFromTarget>, SyncError> {
        let result = self.resolve_sync_target_transactionally(target).await;
        match &result {
            Ok(Some(SyncFromTarget::Existing { .. })) => SpanOutcome::Existing,
            Ok(Some(SyncFromTarget::New(_))) => SpanOutcome::New,
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

    async fn commit_sync(
        &self,
        transferred: SyncFromTransferred,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        let SyncFromTransferred {
            target,
            new_head,
            num_blocks,
            increment,
        } = transferred;

        let committed = match target {
            SyncFromTransferredTarget::Existing {
                dataset,
                expected_head,
            } => self
                .commit_sync_into_existing(&dataset, expected_head.as_ref(), &new_head)
                .await
                .map(|()| expected_head),
            SyncFromTransferredTarget::New {
                alias,
                seed_block,
                seed_block_bytes,
                visibility,
            } => self
                .commit_sync_into_new(
                    &alias,
                    seed_block,
                    CreateDatasetUseCaseOptions {
                        dataset_visibility: visibility,
                        seed_block_bytes: Some(seed_block_bytes),
                        initial_head: Some(new_head.clone()),
                    },
                    maybe_pull_alias,
                )
                .await
                .map(|()| None),
        };

        match committed {
            Ok(old_head) => Ok(Some(SyncResult::Updated {
                old_head,
                new_head,
                num_blocks,
                increment,
            })),
            // The commit reopens an existing target by its ID, so a deleted one is not found
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

    #[tracing::instrument(level = "info", name = DatasetUpdateStepsImpl_transfer_and_commit_sync, skip_all, fields(?request, outcome))]
    async fn transfer_and_commit_sync(
        &self,
        request: SyncFromRequest,
        options: SyncOptions,
        listener: Option<Arc<dyn SyncListener>>,
        maybe_pull_alias: Option<&odf::DatasetRefRemote>,
    ) -> Result<Option<SyncResult>, SyncError> {
        let sync_service = self.sync_service.get().int_err()?;
        let result = match sync_service.sync_from(request, options, listener).await {
            Ok(SyncFromTransfer::UpToDate) => Ok(Some(SyncResult::UpToDate)),
            Ok(SyncFromTransfer::Transferred(transferred)) => {
                self.commit_sync(transferred, maybe_pull_alias).await
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
