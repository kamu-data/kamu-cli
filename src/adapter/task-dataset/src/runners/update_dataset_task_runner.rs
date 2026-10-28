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
use internal_error::InternalError;
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use kamu_task_system::*;
use random_strings::get_random_name;

use super::task_dataset_head_committer::TaskDatasetHeadCommitter;
use crate::{
    DatasetUpdateJob,
    InputDatasetCompactedError,
    TaskDefinitionDatasetUpdate,
    TaskErrorDatasetUpdate,
    TaskResultDatasetUpdate,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetUpdate::TASK_TYPE,
})]
pub struct UpdateDatasetTaskRunner {
    catalog: dill::Catalog,
    polling_ingest_service: Arc<dyn PollingIngestService>,
    transform_executor: Arc<dyn TransformExecutor>,
    sync_service: Arc<dyn SyncService>,
}

#[common_macros::method_names_consts]
impl UpdateDatasetTaskRunner {
    #[tracing::instrument(name = UpdateDatasetTaskRunner_run_update, level = "debug", skip_all)]
    async fn run_update(
        &self,
        task_update: TaskDefinitionDatasetUpdate,
    ) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(?task_update, "Running update task");

        match task_update.job {
            DatasetUpdateJob::Ingest(target_handle) => {
                self.run_ingest_update(&target_handle, task_update.ingest_options)
                    .await
            }
            DatasetUpdateJob::Transform(target_handle) => {
                self.run_transform_update(&target_handle).await
            }
            DatasetUpdateJob::Sync { target, remote_ref } => {
                self.run_sync_update(&target, remote_ref, task_update.sync_options)
                    .await
            }
        }
    }

    /// Reads the target's `HEAD` in a transaction of its own, transfers with
    /// none open, and commits in another
    async fn run_sync_update(
        &self,
        target_handle: &odf::DatasetHandle,
        remote_ref: odf::DatasetRefRemote,
        sync_opts: SyncOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let dst = match self.resolve_sync_target(target_handle).await {
            Ok(Some(dst)) => dst,
            Ok(None) => {
                return Ok(TaskDatasetHeadCommitter::dataset_deleted_outcome(
                    target_handle,
                ));
            }
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Reading the sync target failed");
                return Ok(Self::failed(e.is_recoverable()));
            }
        };
        let sync_request = LocalSyncRequest {
            src: remote_ref,
            dst,
        };

        let transfer = self
            .sync_service
            .transfer_into_local(sync_request, sync_opts, None)
            .await;

        let sync_result = match transfer {
            Ok(SyncTransfer::UpToDate) => Ok(SyncResult::UpToDate),
            Ok(SyncTransfer::Transferred(transferred)) => self.commit_sync(transferred).await,
            Err(e) => Err(e),
        };

        match sync_result {
            Ok(SyncResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate::Updated {
                    old_head,
                    new_head,
                    has_more: false,
                    increment,
                }
                .into_task_result(),
            )),
            Ok(SyncResult::UpToDate) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Sync).into_task_result(),
            )),
            Err(SyncError::HeadMoved(e)) => Ok(TaskDatasetHeadCommitter::head_moved_outcome(
                target_handle.id.clone(),
                e,
            )),
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Sync failed");
                Ok(Self::failed(e.is_recoverable()))
            }
        }
    }

    /// Returns `None` when the target was deleted
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn resolve_sync_target(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<Option<LocalSyncTarget>, SyncError> {
        let target = PullLocalTarget::Existing(target_handle.clone())
            .resolve_sync_target(dataset_registry.as_ref())
            .await?;
        if let Some(target) = &target {
            target.detach_from_transaction();
        }
        Ok(target)
    }

    #[transactional_method1(local_sync_committer: Arc<dyn LocalSyncCommitter>)]
    async fn commit_sync(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError> {
        local_sync_committer.commit(transferred).await
    }

    /// Plans the ingest and resolves its environment variables in one
    /// transaction, then detaches the target for execution. Returns `None`
    /// when the target was deleted
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, polling_ingest_planner: Arc<dyn PollingIngestPlanner>)]
    async fn plan_ingest(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<Option<(ResolvedDataset, PollingIngestPlan, IngestEnvVars)>, PollingIngestPlanError>
    {
        let Some(target) = dataset_registry
            .try_get_dataset_by_ref(&target_handle.id.as_local_ref())
            .await?
        else {
            return Ok(None);
        };

        let plan = polling_ingest_planner.plan_ingest(target.clone()).await?;
        let env_vars = polling_ingest_planner
            .resolve_env_vars(&target_handle.id)
            .await?;

        target.detach_from_transaction();

        Ok(Some((target, plan, env_vars)))
    }

    async fn run_ingest_update(
        &self,
        target_handle: &odf::DatasetHandle,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let (target, plan, env_vars) = match self.plan_ingest(target_handle).await {
            Ok(Some(planned)) => planned,
            Ok(None) => {
                return Ok(TaskDatasetHeadCommitter::dataset_deleted_outcome(
                    target_handle,
                ));
            }
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Ingest planning failed");
                return Ok(Self::failed(e.is_recoverable()));
            }
        };

        let ingest_response = self
            .polling_ingest_service
            .ingest(
                target.clone(),
                plan,
                ingest_options,
                &env_vars,
                &get_random_name(None, 10),
                None,
            )
            .await;

        match ingest_response {
            Ok(PollingIngestResponse {
                result:
                    PollingIngestResult::Updated {
                        old_head,
                        new_head,
                        has_more,
                        increment,
                        ..
                    },
                ..
            }) => {
                if let Some(failed) = TaskDatasetHeadCommitter::new(&self.catalog)
                    .commit(target.get_handle(), &old_head, &new_head)
                    .await?
                {
                    return Ok(failed);
                }

                Ok(TaskOutcome::Success(
                    TaskResultDatasetUpdate::Updated {
                        old_head: Some(old_head),
                        new_head,
                        has_more,
                        increment,
                    }
                    .into_task_result(),
                ))
            }
            Ok(PollingIngestResponse {
                result: PollingIngestResult::UpToDate { uncacheable, .. },
                ..
            }) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::PollingIngest(
                    PollingIngestResultUpToDate { uncacheable },
                ))
                .into_task_result(),
            )),
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Ingest failed");
                Ok(Self::failed(e.is_recoverable()))
            }
        }
    }

    fn transform_up_to_date() -> TaskOutcome {
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Transform).into_task_result(),
        )
    }

    fn failed(recoverable: bool) -> TaskOutcome {
        TaskOutcome::Failed(if recoverable {
            TaskError::empty_recoverable()
        } else {
            TaskError::empty_unrecoverable()
        })
    }

    /// Elaborates in one transaction, then detaches the plan for execution.
    /// Returns `None` when the target was deleted
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, transform_planner: Arc<dyn TransformPlanner>)]
    async fn elaborate_transform(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<Option<(ResolvedDataset, TransformElaboration)>, TransformElaborateError> {
        let Some(target) = dataset_registry
            .try_get_dataset_by_ref(&target_handle.id.as_local_ref())
            .await?
        else {
            return Ok(None);
        };

        let elaboration = transform_planner
            .elaborate_transform(target.clone())
            .await?;

        target.detach_from_transaction();
        if let TransformElaboration::Elaborated(plan) = &elaboration {
            plan.datasets_map.detach_from_transaction();
        }

        Ok(Some((target, elaboration)))
    }

    async fn run_transform_update(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<TaskOutcome, InternalError> {
        let (target, transform_plan) = match self.elaborate_transform(target_handle).await {
            Ok(Some((target, TransformElaboration::Elaborated(transform_plan)))) => {
                (target, transform_plan)
            }
            Ok(None) => {
                return Ok(TaskDatasetHeadCommitter::dataset_deleted_outcome(
                    target_handle,
                ));
            }
            Ok(Some((_, TransformElaboration::UpToDate))) => {
                return Ok(Self::transform_up_to_date());
            }
            // The derivative waits for its input, the same as when it is up to date
            Err(TransformElaborateError::InputNotPulled(e)) => {
                tracing::info!(
                    input = %e.dataset_handle,
                    "Not processing because one of the inputs has no data yet",
                );
                return Ok(Self::transform_up_to_date());
            }
            Err(TransformElaborateError::InvalidInputInterval(e)) => {
                return Ok(TaskOutcome::Failed(
                    TaskErrorDatasetUpdate::InputDatasetCompacted(InputDatasetCompactedError {
                        dataset_id: e.input_dataset.id,
                    })
                    .into_task_error(),
                ));
            }
            Err(
                e @ (TransformElaborateError::TransformNotDefined(_)
                | TransformElaborateError::InputsChanged(_)
                | TransformElaborateError::Internal(_)),
            ) => {
                tracing::error!(error = ?e, error_msg = %e, "Transform elaboration failed");
                return Ok(Self::failed(e.is_recoverable()));
            }
        };

        let (_, execution_result) = self
            .transform_executor
            .execute_transform(
                target.clone(),
                transform_plan,
                &get_random_name(None, 10),
                None,
            )
            .await;

        match execution_result {
            Ok(TransformResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => {
                if let Some(failed) = TaskDatasetHeadCommitter::new(&self.catalog)
                    .commit(target.get_handle(), &old_head, &new_head)
                    .await?
                {
                    return Ok(failed);
                }

                Ok(TaskOutcome::Success(
                    TaskResultDatasetUpdate::Updated {
                        old_head: Some(old_head),
                        new_head,
                        has_more: false,
                        increment,
                    }
                    .into_task_result(),
                ))
            }
            Ok(TransformResult::UpToDate) => Ok(Self::transform_up_to_date()),
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Transform execution failed");
                Ok(Self::failed(e.is_recoverable()))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for UpdateDatasetTaskRunner {
    async fn run_task(
        &self,
        task_definition: TaskDefinition,
    ) -> Result<kamu_task_system::TaskOutcome, kamu_task_system::InternalError> {
        let task_update = task_definition
            .downcast::<TaskDefinitionDatasetUpdate>()
            .expect("Mismatched task type for UpdateDatasetTaskRunner");

        self.run_update(*task_update).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
