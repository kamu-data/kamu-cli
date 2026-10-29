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
use kamu_core::{CompactionExecutor, CompactionPlan, CompactionResult, DatasetUpdateSteps};
use kamu_datasets::ResolvedDataset;
use kamu_task_system::*;

use super::task_head_commit::HeadCommitTaskOutcome as _;
use crate::{
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskResultDatasetHardCompact,
    TaskResultDatasetResetToMetadata,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs both compaction modes, hard compaction and reset to metadata, which
/// differ only in their plan and in the result type they report.
#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetHardCompact::TASK_TYPE,
})]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetResetToMetadata::TASK_TYPE,
})]
pub struct CompactionDatasetTaskRunner {
    compaction_executor: Arc<dyn CompactionExecutor>,
    update_steps: Arc<dyn DatasetUpdateSteps>,
}

#[common_macros::method_names_consts]
impl CompactionDatasetTaskRunner {
    #[tracing::instrument(name = CompactionDatasetTaskRunner_run_compaction, level = "debug", skip_all)]
    async fn run_compaction(
        &self,
        target: ResolvedDataset,
        plan: CompactionPlan,
        into_task_result: fn(CompactionResult) -> TaskResult,
    ) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(dataset_handle = %target.get_handle(), ?plan, "Running compaction task");

        // Run compaction execution without transaction
        let compaction_result = match self
            .compaction_executor
            .execute(target.clone(), plan, None)
            .await
        {
            Ok(compaction_result) => compaction_result,
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Compaction failed",
                );

                return Ok(TaskOutcome::Failed(TaskError::empty_recoverable()));
            }
        };

        if let CompactionResult::Success {
            old_head, new_head, ..
        } = &compaction_result
            && let Some(failed) = self
                .update_steps
                .commit_head(target.get_handle(), old_head, new_head)
                .await?
                .into_failed_task_outcome(target.get_handle())
        {
            return Ok(failed);
        }

        Ok(TaskOutcome::Success(into_task_result(compaction_result)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for CompactionDatasetTaskRunner {
    async fn run_task(
        &self,
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<kamu_task_system::TaskOutcome, kamu_task_system::InternalError> {
        match task_definition.task_type() {
            TaskDefinitionDatasetHardCompact::TASK_TYPE => {
                let task_compact = task_definition
                    .downcast::<TaskDefinitionDatasetHardCompact>()
                    .expect("Mismatched task type for CompactionDatasetTaskRunner");

                self.run_compaction(task_compact.target, task_compact.compaction_plan, |r| {
                    TaskResultDatasetHardCompact {
                        compaction_result: r,
                    }
                    .into_task_result()
                })
                .await
            }
            TaskDefinitionDatasetResetToMetadata::TASK_TYPE => {
                let task_reset = task_definition
                    .downcast::<TaskDefinitionDatasetResetToMetadata>()
                    .expect("Mismatched task type for CompactionDatasetTaskRunner");

                self.run_compaction(
                    task_reset.target,
                    task_reset.compaction_metadata_only_plan,
                    |r| {
                        TaskResultDatasetResetToMetadata {
                            compaction_metadata_only_result: r,
                        }
                        .into_task_result()
                    },
                )
                .await
            }
            unexpected => {
                panic!("Unexpected task type for CompactionDatasetTaskRunner: {unexpected}")
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
