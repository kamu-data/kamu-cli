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
use kamu_core::{ResetError, ResetExecutionError, ResetJob, ResetPlanningError};
use kamu_task_system::*;

use super::task_reference_update::{
    dataset_deleted_outcome,
    failed_outcome,
    operation_not_allowed_outcome,
    ref_cas_failed_outcome,
};
use super::task_span_outcome::TaskSpanOutcome;
use crate::{
    DatasetResetOldHeadMismatch,
    TaskDefinitionDatasetReset,
    TaskErrorDatasetReset,
    TaskResultDatasetReset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetReset::TASK_TYPE,
})]
pub struct ResetDatasetTaskRunner {
    reset_job: Arc<dyn ResetJob>,
}

#[common_macros::method_names_consts]
impl ResetDatasetTaskRunner {
    #[tracing::instrument(name = ResetDatasetTaskRunner_run_reset, level = "info", skip_all, fields(target = %task_reset.dataset_handle, outcome))]
    async fn run_reset(
        &self,
        task_reset: TaskDefinitionDatasetReset,
    ) -> Result<TaskOutcome, InternalError> {
        let target = &task_reset.dataset_handle;
        let outcome = match self
            .reset_job
            .run(
                target,
                task_reset.new_head_hash.as_ref(),
                task_reset.old_head_hash.as_ref(),
            )
            .await
        {
            Ok(reset_result) => Ok(TaskOutcome::Success(
                TaskResultDatasetReset { reset_result }.into_task_result(),
            )),
            Err(e) => Self::failed_outcome(target, e),
        };
        TaskSpanOutcome::record(&outcome);
        outcome
    }

    fn failed_outcome(
        target: &odf::DatasetHandle,
        error: ResetError,
    ) -> Result<TaskOutcome, InternalError> {
        use odf::dataset::SetChainRefError as E;

        match error {
            ResetError::NotFound(_) => Ok(dataset_deleted_outcome(target)),
            ResetError::Planning(ResetPlanningError::OldHeadMismatch(e)) => {
                tracing::warn!(error = ?e, error_msg = %e, "The dataset is not at the expected old head");
                Ok(TaskOutcome::Failed(
                    TaskErrorDatasetReset::OldHeadMismatch(DatasetResetOldHeadMismatch {
                        expected_head: e.old_head,
                        actual_head: e.current_head,
                    })
                    .into_task_error(),
                ))
            }
            ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::BlockNotFound(_))) => {
                Ok(TaskOutcome::Failed(
                    TaskErrorDatasetReset::ResetHeadNotFound.into_task_error(),
                ))
            }
            ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::CASFailed(e))) => {
                Ok(ref_cas_failed_outcome(target, e))
            }
            ResetError::Access(e)
            | ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::Access(e))) => {
                Ok(operation_not_allowed_outcome(&e))
            }
            e @ (ResetError::Planning(ResetPlanningError::Internal(_))
            | ResetError::Execution(ResetExecutionError::SetReferenceFailed(E::Internal(_)))) => {
                Ok(failed_outcome(&e, true, "Reset failed"))
            }
            ResetError::Internal(e) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for ResetDatasetTaskRunner {
    async fn run_task(
        &self,
        task_definition: TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        let task_reset = task_definition
            .downcast::<TaskDefinitionDatasetReset>()
            .expect("Mismatched task type for ResetDatasetTaskRunner");

        self.run_reset(*task_reset).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
