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
use kamu_core::{CompactionError, CompactionJob, CompactionOptions, CompactionResult};
use kamu_task_system::*;

use super::task_reference_update::{
    dataset_deleted_outcome,
    failed_outcome,
    operation_not_allowed_outcome,
    ref_cas_failed_outcome,
};
use super::task_span_outcome::TaskSpanOutcome;
use crate::{
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
    TaskResultDatasetHardCompact,
    TaskResultDatasetResetToMetadata,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs hard compaction and reset-to-metadata tasks, which differ only in
/// their options and in the result type they report.
#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetHardCompact::TASK_TYPE,
})]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetResetToMetadata::TASK_TYPE,
})]
pub struct CompactionDatasetTaskRunner {
    compaction_job: Arc<dyn CompactionJob>,
}

#[common_macros::method_names_consts]
impl CompactionDatasetTaskRunner {
    #[tracing::instrument(name = CompactionDatasetTaskRunner_run_compaction, level = "info", skip_all, fields(%target, outcome))]
    async fn run_compaction(
        &self,
        target: odf::DatasetHandle,
        options: CompactionOptions,
        into_task_result: fn(CompactionResult) -> TaskResult,
    ) -> Result<TaskOutcome, InternalError> {
        let outcome = match self.compaction_job.run(&target, options, None).await {
            Ok(result) => Ok(TaskOutcome::Success(into_task_result(result))),
            Err(e) => Self::failed_outcome(&target, e),
        };
        TaskSpanOutcome::record(&outcome);
        outcome
    }

    fn failed_outcome(
        target: &odf::DatasetHandle,
        error: CompactionError,
    ) -> Result<TaskOutcome, InternalError> {
        match error {
            CompactionError::NotFound(_) => Ok(dataset_deleted_outcome(target)),
            CompactionError::RefCASFailed(e) => Ok(ref_cas_failed_outcome(target, e)),
            CompactionError::Access(e) => Ok(operation_not_allowed_outcome(&e)),
            CompactionError::Planning(e) => Ok(failed_outcome(
                &e,
                e.is_recoverable(),
                "Compaction planning failed",
            )),
            CompactionError::Execution(e) => Ok(failed_outcome(&e, true, "Compaction failed")),
            CompactionError::Internal(e) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for CompactionDatasetTaskRunner {
    async fn run_task(
        &self,
        task_definition: TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        match task_definition.task_type() {
            TaskDefinitionDatasetHardCompact::TASK_TYPE => {
                let task_compact = task_definition
                    .downcast::<TaskDefinitionDatasetHardCompact>()
                    .expect("Mismatched task type for CompactionDatasetTaskRunner");

                self.run_compaction(task_compact.target, task_compact.options, |r| {
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
                    CompactionOptions {
                        max_slice_size: None,
                        max_slice_records: None,
                        keep_metadata_only: true,
                    },
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
