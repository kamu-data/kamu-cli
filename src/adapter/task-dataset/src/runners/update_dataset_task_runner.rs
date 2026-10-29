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
use kamu_task_system::*;
use random_strings::get_random_name;

use super::task_head_commit::{
    HeadCommitTaskOutcome as _,
    dataset_deleted_outcome,
    head_moved_outcome,
};
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
    polling_ingest_service: Arc<dyn PollingIngestService>,
    transform_executor: Arc<dyn TransformExecutor>,
    update_steps: Arc<dyn DatasetUpdateSteps>,
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
        let dst = match self
            .update_steps
            .resolve_sync_target(&PullLocalTarget::Existing(target_handle.clone()))
            .await
        {
            Ok(Some(dst)) => dst,
            Ok(None) => {
                return Ok(dataset_deleted_outcome(target_handle));
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

        let sync_result = self
            .update_steps
            .transfer_and_commit_sync(sync_request, sync_opts, None, None)
            .await;

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
            Err(SyncError::HeadMoved(SyncHeadMovedError { heads: Some(e) })) => {
                Ok(head_moved_outcome(target_handle.id.clone(), e))
            }
            Err(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Sync failed");
                Ok(Self::failed(e.is_recoverable()))
            }
        }
    }

    async fn run_ingest_update(
        &self,
        target_handle: &odf::DatasetHandle,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let PlannedIngest {
            target,
            plan,
            env_vars,
        } = match self.update_steps.plan_ingest(target_handle).await {
            Ok(Some(planned)) => planned,
            Ok(None) => {
                return Ok(dataset_deleted_outcome(target_handle));
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
            Ok(PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more,
                increment,
                ..
            }) => {
                if let Some(failed) = self
                    .update_steps
                    .commit_head(target.get_handle(), &old_head, &new_head)
                    .await?
                    .into_failed_task_outcome(target.get_handle())
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
            Ok(PollingIngestResult::UpToDate { uncacheable }) => Ok(TaskOutcome::Success(
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
        TaskOutcome::Failed(TaskError::empty(recoverable))
    }

    async fn run_transform_update(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<TaskOutcome, InternalError> {
        let (target, transform_plan) =
            match self.update_steps.elaborate_transform(target_handle).await {
                Ok(Some(ElaboratedTransform {
                    target,
                    elaboration: TransformElaboration::Elaborated(transform_plan),
                })) => (target, transform_plan),
                Ok(None) => {
                    return Ok(dataset_deleted_outcome(target_handle));
                }
                Ok(Some(ElaboratedTransform {
                    elaboration: TransformElaboration::UpToDate,
                    ..
                })) => {
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
                if let Some(failed) = self
                    .update_steps
                    .commit_head(target.get_handle(), &old_head, &new_head)
                    .await?
                    .into_failed_task_outcome(target.get_handle())
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
