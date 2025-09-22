// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use internal_error::InternalError;
use kamu_core::engine::EngineError;
use kamu_core::*;
use kamu_task_system::*;

use crate::{
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
    transform_elaboration_service: Arc<dyn TransformElaborationService>,
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

        match task_update.pull_job {
            PullPlanIterationJob::Ingest(ingest_item) => {
                self.run_ingest_update(ingest_item, task_update.pull_options.ingest_options)
                    .await
            }
            PullPlanIterationJob::Transform(transform_item) => {
                self.run_transform_update(transform_item).await
            }
            PullPlanIterationJob::Sync(sync_item) => {
                self.run_sync_update(
                    *sync_item.sync_request,
                    task_update.pull_options.sync_options,
                )
                .await
            }
        }
    }

    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn run_sync_update(
        &self,
        mut sync_request: SyncRequest,
        sync_opts: SyncOptions,
    ) -> Result<TaskOutcome, InternalError> {
        sync_request
            .src
            .refresh_dataset_from_registry(dataset_registry.as_ref())
            .await;
        sync_request
            .dst
            .refresh_dataset_from_registry(dataset_registry.as_ref())
            .await;

        let sync_response = self.sync_service.sync(sync_request, sync_opts, None).await;
        match sync_response {
            Ok(sync_result) => Ok(TaskOutcome::Success(
                TaskResultDatasetUpdate {
                    pull_result: sync_result.into(),
                }
                .into_task_result(),
            )),
            Err(_) =>
            // TODO: classify sync errors as recoverable/unrecoverable
            {
                Ok(TaskOutcome::Failed(TaskError::empty_recoverable()))
            }
        }
    }

    async fn run_ingest_update(
        &self,
        ingest_item: PullIngestItem,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let ingest_response = self
            .polling_ingest_service
            .ingest(
                ingest_item.target.clone(),
                ingest_item.metadata_state,
                ingest_options,
                None,
            )
            .await;
        match ingest_response {
            Ok(ingest_result) => {
                // Do we have a new HEAD suggestion?
                if let PollingIngestResult::Updated {
                    old_head, new_head, ..
                } = &ingest_result
                {
                    // Update the reference transactionally
                    self.update_dataset_head(
                        ingest_item.target.get_handle(),
                        Some(old_head),
                        new_head,
                    )
                    .await?;
                }

                Ok(TaskOutcome::Success(
                    TaskResultDatasetUpdate {
                        pull_result: ingest_result.into(),
                    }
                    .into_task_result(),
                ))
            }
            Err(e) => match e {
                PollingIngestError::ParameterNotFound(_)
                | PollingIngestError::ReadError(_)
                | PollingIngestError::EngineError(EngineError::InvalidQuery(_))
                | PollingIngestError::BadInputSchema(_)
                | PollingIngestError::IncompatibleSchema(_)
                | PollingIngestError::InvalidParameterFormat(_)
                | PollingIngestError::TemplateError(_) => {
                    Ok(TaskOutcome::Failed(TaskError::empty_unrecoverable()))
                }

                PollingIngestError::CommitError(_)
                | PollingIngestError::DataValidation(_)
                | PollingIngestError::EngineError(_)
                | PollingIngestError::EngineProvisioningError(_)
                | PollingIngestError::ImagePull(_)
                | PollingIngestError::Internal(_)
                | PollingIngestError::MergeError(_)
                | PollingIngestError::NotFound { .. }
                | PollingIngestError::PipeError(_)
                | PollingIngestError::ProcessError(_)
                | PollingIngestError::Unreachable { .. } => {
                    Ok(TaskOutcome::Failed(TaskError::empty_recoverable()))
                }
            },
        }
    }

    async fn run_transform_update(
        &self,
        transform_item: PullTransformItem,
    ) -> Result<TaskOutcome, InternalError> {
        let transform_elaboration = match self
            .transform_elaboration_service
            .elaborate_transform(
                transform_item.target.clone(),
                transform_item.plan,
                TransformOptions::default(),
                None,
            )
            .await
        {
            Ok(request) => Ok(request),
            // Special case: input dataset compacted
            Err(TransformElaborateError::InvalidInputInterval(e)) => {
                return Ok(TaskOutcome::Failed(
                    TaskErrorDatasetUpdate::InputDatasetCompacted(InputDatasetCompactedError {
                        dataset_id: e.input_dataset_id,
                    })
                    .into_task_error(),
                ));
            }
            Err(e) => {
                tracing::error!(error = ?e, "Transform elaboration failed");
                match e {
                    TransformElaborateError::InputSchemaNotDefined(_)
                    | TransformElaborateError::InvalidInputInterval(_) => {
                        return Ok(TaskOutcome::Failed(TaskError::empty_unrecoverable()));
                    }

                    TransformElaborateError::Internal(_) => {
                        return Ok(TaskOutcome::Failed(TaskError::empty_recoverable()));
                    }
                }
            }
        }?;

        match transform_elaboration {
            TransformElaboration::Elaborated(transform_plan) => {
                let (_, execution_result) = self
                    .transform_executor
                    .execute_transform(transform_item.target.clone(), transform_plan, None)
                    .await;

                match execution_result {
                    Ok(transform_result) => {
                        if let TransformResult::Updated { old_head, new_head } = &transform_result {
                            // Update the reference transactionally
                            self.update_dataset_head(
                                transform_item.target.get_handle(),
                                Some(old_head),
                                new_head,
                            )
                            .await?;
                        }

                        Ok(TaskOutcome::Success(
                            TaskResultDatasetUpdate {
                                pull_result: transform_result.into(),
                            }
                            .into_task_result(),
                        ))
                    }
                    Err(e) => {
                        tracing::error!(error = ?e, "Transform execution failed");
                        match e {
                            TransformExecuteError::EngineError(EngineError::InvalidQuery(_)) => {
                                Ok(TaskOutcome::Failed(TaskError::empty_unrecoverable()))
                            }

                            TransformExecuteError::CommitError(_)
                            | TransformExecuteError::EngineError(_)
                            | TransformExecuteError::EngineProvisioningError(_)
                            | TransformExecuteError::Internal(_) => {
                                Ok(TaskOutcome::Failed(TaskError::empty_recoverable()))
                            }
                        }
                    }
                }
            }
            TransformElaboration::UpToDate => Ok(TaskOutcome::Success(TaskResult::empty())),
        }
    }

    #[tracing::instrument(name = UpdateDatasetTaskRunner_update_dataset_head, level = "debug", skip_all, fields(%dataset_handle, %new_head))]
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn update_dataset_head(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), InternalError> {
        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        target
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(old_head),
                },
            )
            .await
            .int_err()
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
