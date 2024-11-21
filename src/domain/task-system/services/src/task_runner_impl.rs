// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::InternalError;
use kamu_core::*;
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskRunnerImpl {
    polling_ingest_service: Arc<dyn PollingIngestService>,
    transform_elaboration_service: Arc<dyn TransformElaborationService>,
    transform_execution_service: Arc<dyn TransformExecutionService>,
    reset_service: Arc<dyn ResetService>,
    compaction_service: Arc<dyn CompactionService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskRunner)]
impl TaskRunnerImpl {
    pub fn new(
        polling_ingest_service: Arc<dyn PollingIngestService>,
        transform_elaboration_service: Arc<dyn TransformElaborationService>,
        transform_execution_service: Arc<dyn TransformExecutionService>,
        reset_service: Arc<dyn ResetService>,
        compaction_service: Arc<dyn CompactionService>,
    ) -> Self {
        Self {
            polling_ingest_service,
            transform_elaboration_service,
            transform_execution_service,
            reset_service,
            compaction_service,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?task_probe))]
    async fn run_probe(
        &self,
        task_probe: TaskDefinitionProbe,
    ) -> Result<TaskOutcome, InternalError> {
        if let Some(busy_time) = task_probe.probe.busy_time {
            tokio::time::sleep(busy_time).await;
        }
        Ok(task_probe
            .probe
            .end_with_outcome
            .clone()
            .unwrap_or(TaskOutcome::Success(TaskResult::Empty)))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?task_update))]
    async fn run_update(
        &self,
        task_update: TaskDefinitionUpdate,
    ) -> Result<TaskOutcome, InternalError> {
        match task_update.pull_job {
            PullPlanIterationJob::Ingest(ingest_item) => {
                self.run_ingest_update(ingest_item, task_update.pull_options.ingest_options)
                    .await
            }
            PullPlanIterationJob::Transform(transform_item) => {
                self.run_transform_update(transform_item).await
            }
            PullPlanIterationJob::Sync(_) => {
                unreachable!("No Sync jobs possible from update requests");
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
            .ingest(ingest_item.target, ingest_options, None)
            .await;
        match ingest_response {
            Ok(ingest_result) => Ok(TaskOutcome::Success(TaskResult::UpdateDatasetResult(
                TaskUpdateDatasetResult {
                    pull_result: ingest_result.into(),
                },
            ))),
            Err(_) => Ok(TaskOutcome::Failed(TaskError::Empty)),
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
                return Ok(TaskOutcome::Failed(TaskError::UpdateDatasetError(
                    UpdateDatasetTaskError::InputDatasetCompacted(InputDatasetCompactedError {
                        dataset_id: e.input_dataset_id,
                    }),
                )));
            }
            Err(e) => {
                tracing::error!(error = ?e, "Update failed");
                Err("Transform request elaboration failed".int_err())
            }
        }?;

        match transform_elaboration {
            TransformElaboration::Elaborated(transform_plan) => {
                let (_, execution_result) = self
                    .transform_execution_service
                    .execute_transform(transform_item.target, transform_plan, None)
                    .await;

                match execution_result {
                    Ok(transform_result) => Ok(TaskOutcome::Success(
                        TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                            pull_result: transform_result.into(),
                        }),
                    )),
                    Err(e) => {
                        tracing::error!(error = ?e, "Transform execution failed");
                        Ok(TaskOutcome::Failed(TaskError::Empty))
                    }
                }
            }
            TransformElaboration::UpToDate => Ok(TaskOutcome::Success(TaskResult::Empty)),
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?task_reset))]
    async fn run_reset(
        &self,
        task_reset: TaskDefinitionReset,
    ) -> Result<TaskOutcome, InternalError> {
        let reset_result_maybe = self
            .reset_service
            .reset_dataset(
                task_reset.target,
                task_reset.new_head_hash.as_ref(),
                task_reset.old_head_hash.as_ref(),
            )
            .await;
        match reset_result_maybe {
            Ok(new_head) => Ok(TaskOutcome::Success(TaskResult::ResetDatasetResult(
                TaskResetDatasetResult { new_head },
            ))),
            Err(err) => match err {
                ResetError::BlockNotFound(_) => Ok(TaskOutcome::Failed(
                    TaskError::ResetDatasetError(ResetDatasetTaskError::ResetHeadNotFound),
                )),
                err => {
                    tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "Reset failed",
                    );

                    Ok(TaskOutcome::Failed(TaskError::Empty))
                }
            },
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?task_compact))]
    async fn run_hard_compaction(
        &self,
        task_compact: TaskDefinitionHardCompact,
    ) -> Result<TaskOutcome, InternalError> {
        let compaction_result = self
            .compaction_service
            .compact_dataset(task_compact.target, task_compact.compaction_options, None)
            .await;

        match compaction_result {
            Ok(result) => Ok(TaskOutcome::Success(TaskResult::CompactionDatasetResult(
                result.into(),
            ))),
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Hard compaction failed",
                );

                Ok(TaskOutcome::Failed(TaskError::Empty))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for TaskRunnerImpl {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_task(
        &self,
        task_definition: TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(?task_definition, "Running task");

        let task_outcome = match task_definition {
            TaskDefinition::Probe(td_probe) => self.run_probe(td_probe).await?,
            TaskDefinition::Update(td_update) => self.run_update(td_update).await?,
            TaskDefinition::Reset(td_reset) => self.run_reset(td_reset).await?,
            TaskDefinition::HardCompact(td_compact) => self.run_hard_compaction(td_compact).await?,
        };

        Ok(task_outcome)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
