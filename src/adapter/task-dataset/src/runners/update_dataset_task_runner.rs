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

use super::task_failure_outcomes::{
    dataset_deleted_outcome,
    failed_outcome,
    operation_not_allowed_outcome,
    ref_cas_failed_outcome,
    ref_moved_outcome,
};
use super::task_span_outcome::TaskSpanOutcome;
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
    polling_ingest_job: Arc<dyn PollingIngestJob>,
    transform_job: Arc<dyn TransformJob>,
    sync_from_job: Arc<dyn SyncFromJob>,
}

#[common_macros::method_names_consts]
impl UpdateDatasetTaskRunner {
    #[tracing::instrument(name = UpdateDatasetTaskRunner_run_update, level = "info", skip_all, fields(target = %task_update.job.target(), outcome))]
    async fn run_update(
        &self,
        task_update: TaskDefinitionDatasetUpdate,
    ) -> Result<TaskOutcome, InternalError> {
        let outcome = self.run_update_job(task_update).await;
        TaskSpanOutcome::record(&outcome);
        outcome
    }

    async fn run_update_job(
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
                self.run_sync_update(&target, &remote_ref, task_update.sync_options)
                    .await
            }
        }
    }

    async fn run_ingest_update(
        &self,
        target_handle: &odf::DatasetHandle,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        match self
            .polling_ingest_job
            .run(target_handle, ingest_options, None, None)
            .await
        {
            Ok(PollingIngestResult::Updated {
                old_head,
                new_head,
                has_more,
                increment,
                ..
            }) => Ok(Self::updated(Some(old_head), new_head, has_more, increment)),
            Ok(PollingIngestResult::UpToDate { uncacheable }) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::PollingIngest(
                    PollingIngestResultUpToDate { uncacheable },
                ))
                .into_task_result(),
            )),
            Err(PollingIngestJobError::NotFound(_)) => Ok(dataset_deleted_outcome(target_handle)),
            Err(PollingIngestJobError::RefCASFailed(e)) => {
                Ok(ref_cas_failed_outcome(target_handle, e))
            }
            Err(PollingIngestJobError::Access(e)) => Ok(operation_not_allowed_outcome(&e)),
            Err(PollingIngestJobError::Planning(e)) => Ok(failed_outcome(
                &e,
                e.is_recoverable(),
                "Ingest planning failed",
            )),
            Err(PollingIngestJobError::Ingest(e)) => {
                Ok(failed_outcome(&e, e.is_recoverable(), "Ingest failed"))
            }
            Err(PollingIngestJobError::Internal(e)) => Err(e),
        }
    }

    async fn run_transform_update(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<TaskOutcome, InternalError> {
        match self.transform_job.run(target_handle, None).await {
            Ok(TransformResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => Ok(Self::updated(Some(old_head), new_head, false, increment)),
            Ok(TransformResult::UpToDate) => Ok(Self::transform_up_to_date()),
            // The derivative waits for its input, the same as when it is up to date
            Err(TransformJobError::Elaborate(TransformElaborateError::InputNotPulled(e))) => {
                tracing::info!(
                    input = %e.dataset_handle,
                    "Not processing because one of the inputs has no data yet",
                );
                Ok(Self::transform_up_to_date())
            }
            Err(TransformJobError::Elaborate(TransformElaborateError::InvalidInputInterval(e))) => {
                Ok(TaskOutcome::Failed(
                    TaskErrorDatasetUpdate::InputDatasetCompacted(InputDatasetCompactedError {
                        dataset_id: e.input_dataset.id,
                    })
                    .into_task_error(),
                ))
            }
            Err(TransformJobError::Elaborate(
                e @ (TransformElaborateError::TransformNotDefined(_)
                | TransformElaborateError::InputsChanged(_)
                | TransformElaborateError::Internal(_)),
            )) => Ok(failed_outcome(
                &e,
                e.is_recoverable(),
                "Transform elaboration failed",
            )),
            Err(TransformJobError::Execute(e)) => Ok(failed_outcome(
                &e,
                e.is_recoverable(),
                "Transform execution failed",
            )),
            Err(TransformJobError::NotFound(_)) => Ok(dataset_deleted_outcome(target_handle)),
            Err(TransformJobError::RefCASFailed(e)) => Ok(ref_cas_failed_outcome(target_handle, e)),
            Err(TransformJobError::Access(e)) => Ok(operation_not_allowed_outcome(&e)),
            Err(TransformJobError::Internal(e)) => Err(e),
        }
    }

    async fn run_sync_update(
        &self,
        target_handle: &odf::DatasetHandle,
        remote_ref: &odf::DatasetRefRemote,
        sync_options: SyncOptions,
    ) -> Result<TaskOutcome, InternalError> {
        match self
            .sync_from_job
            .run(
                &PullLocalTarget::Existing(target_handle.clone()),
                remote_ref,
                sync_options,
                None,
            )
            .await
        {
            Ok(SyncResult::Updated {
                old_head,
                new_head,
                increment,
                ..
            }) => Ok(Self::updated(old_head, new_head, false, increment)),
            Ok(SyncResult::UpToDate) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Sync).into_task_result(),
            )),
            Err(SyncFromJobError::TargetDeleted(_)) => Ok(dataset_deleted_outcome(target_handle)),
            Err(SyncFromJobError::Sync(SyncError::RefMoved(SyncRefMovedError {
                block_ref,
                reported,
            }))) => Ok(ref_moved_outcome(
                target_handle.id.clone(),
                block_ref,
                reported,
            )),
            Err(SyncFromJobError::Sync(
                e @ (SyncError::DatasetNotFound(_)
                | SyncError::RefCollision(_)
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
                | SyncError::Access(_)
                | SyncError::Internal(_)),
            )) => Ok(failed_outcome(&e, e.is_recoverable(), "Sync failed")),
        }
    }

    fn updated(
        old_head: Option<odf::Multihash>,
        new_head: odf::Multihash,
        has_more: bool,
        increment: odf::dataset::MetadataChainIncrementInterval,
    ) -> TaskOutcome {
        TaskOutcome::Success(
            TaskResultDatasetUpdate::Updated {
                old_head,
                new_head,
                has_more,
                increment,
            }
            .into_task_result(),
        )
    }

    fn transform_up_to_date() -> TaskOutcome {
        TaskOutcome::Success(
            TaskResultDatasetUpdate::UpToDate(PullResultUpToDate::Transform).into_task_result(),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for UpdateDatasetTaskRunner {
    async fn run_task(
        &self,
        task_definition: TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        let task_update = task_definition
            .downcast::<TaskDefinitionDatasetUpdate>()
            .expect("Mismatched task type for UpdateDatasetTaskRunner");

        self.run_update(*task_update).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
