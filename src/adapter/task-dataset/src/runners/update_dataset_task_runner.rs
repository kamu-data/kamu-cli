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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{DatasetRegistry, ResolvedDataset};
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
                self.run_ingest_update(&target_handle, task_update.pull_options.ingest_options)
                    .await
            }
            DatasetUpdateJob::Transform(target_handle) => {
                self.run_transform_update(&target_handle).await
            }
            DatasetUpdateJob::Sync(sync_item) => {
                self.run_sync_update(
                    *sync_item.sync_request,
                    task_update.pull_options.sync_options,
                )
                .await
            }
        }
    }

    /// Re-reads the destination's `HEAD` in a transaction of its own,
    /// transfers with none open, and commits in another
    async fn run_sync_update(
        &self,
        sync_request: LocalSyncRequest,
        sync_opts: SyncOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let sync_request = self.plan_sync(sync_request).await?;

        let dst_id = match &sync_request.dst {
            LocalSyncTarget::Existing { dataset, .. } => dataset.get_id().clone(),
            LocalSyncTarget::New(alias) => {
                return InternalError::bail(format!(
                    "An update task syncs into an existing dataset, but {alias} does not exist"
                ));
            }
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
            Err(SyncError::HeadMoved(e)) => {
                Ok(TaskDatasetHeadCommitter::head_moved_outcome(dst_id, e))
            }
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Sync failed");
                Ok(Self::failed(e.is_recoverable()))
            }
        }
    }

    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn plan_sync(
        &self,
        sync_request: LocalSyncRequest,
    ) -> Result<LocalSyncRequest, InternalError> {
        let LocalSyncRequest { mut src, dst } = sync_request;

        src.refresh_dataset_from_registry(dataset_registry.as_ref())
            .await;
        src.detach_from_transaction();

        let dst = match dst {
            LocalSyncTarget::Existing { dataset, .. } => {
                let dataset = dataset_registry
                    .get_dataset_by_handle(dataset.get_handle())
                    .await;
                let target = LocalSyncTarget::existing(dataset).await.int_err()?;
                target.detach_from_transaction();
                target
            }
            new @ LocalSyncTarget::New(_) => new,
        };

        Ok(LocalSyncRequest { src, dst })
    }

    #[transactional_method1(local_sync_committer: Arc<dyn LocalSyncCommitter>)]
    async fn commit_sync(&self, transferred: TransferredSync) -> Result<SyncResult, SyncError> {
        local_sync_committer.commit(transferred).await
    }

    /// Plans the ingest and resolves its environment variables in one
    /// transaction, then detaches the target for execution
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, polling_ingest_planner: Arc<dyn PollingIngestPlanner>)]
    async fn plan_ingest(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<(ResolvedDataset, PollingIngestPlan, IngestEnvVars), PollingIngestPlanError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;

        let plan = polling_ingest_planner.plan_ingest(target.clone()).await?;
        let env_vars = polling_ingest_planner
            .resolve_env_vars(&target_handle.id)
            .await?;

        target.detach_from_transaction();

        Ok((target, plan, env_vars))
    }

    async fn run_ingest_update(
        &self,
        target_handle: &odf::DatasetHandle,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let (target, plan, env_vars) = match self.plan_ingest(target_handle).await {
            Ok(planned) => planned,
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

    /// Elaborates in one transaction, then detaches the plan for execution
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, transform_planner: Arc<dyn TransformPlanner>)]
    async fn elaborate_transform(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<(ResolvedDataset, TransformElaboration), TransformElaborateError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;

        let elaboration = transform_planner
            .elaborate_transform(target.clone())
            .await?;

        target.detach_from_transaction();
        if let TransformElaboration::Elaborated(plan) = &elaboration {
            plan.datasets_map.detach_from_transaction();
        }

        Ok((target, elaboration))
    }

    async fn run_transform_update(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<TaskOutcome, InternalError> {
        let (target, transform_plan) = match self.elaborate_transform(target_handle).await {
            Ok((target, TransformElaboration::Elaborated(transform_plan))) => {
                (target, transform_plan)
            }
            Ok((_, TransformElaboration::UpToDate)) => {
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
