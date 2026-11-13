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
use kamu_core::{ResetExecutionError, ResetExecutor};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt};
use kamu_task_system::*;

use super::task_head_commit::{
    dataset_deleted_outcome,
    head_moved_outcome,
    head_not_allowed_outcome,
};
use super::task_span_outcome::TaskSpanOutcome;
use crate::{TaskDefinitionDatasetReset, TaskErrorDatasetReset, TaskResultDatasetReset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetReset::TASK_TYPE,
})]
pub struct ResetDatasetTaskRunner {
    catalog: dill::Catalog,
    reset_executor: Arc<dyn ResetExecutor>,
}

#[common_macros::method_names_consts]
impl ResetDatasetTaskRunner {
    #[tracing::instrument(name = ResetDatasetTaskRunner_run_reset, level = "info", skip_all, fields(target = %task_reset.dataset_handle, outcome))]
    async fn run_reset(
        &self,
        task_reset: TaskDefinitionDatasetReset,
    ) -> Result<TaskOutcome, InternalError> {
        let outcome = self.run_reset_job(task_reset).await;
        TaskSpanOutcome::record(&outcome);
        outcome
    }

    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn run_reset_job(
        &self,
        task_reset: TaskDefinitionDatasetReset,
    ) -> Result<TaskOutcome, InternalError> {
        let Some(target) = dataset_registry
            .try_get_dataset_by_ref(&task_reset.dataset_handle.as_local_ref())
            .await?
        else {
            return Ok(dataset_deleted_outcome(&task_reset.dataset_handle));
        };

        let reset_result_maybe = self
            .reset_executor
            .execute(target, task_reset.reset_plan)
            .await;

        match reset_result_maybe {
            Ok(reset_result) => Ok(TaskOutcome::Success(
                TaskResultDatasetReset { reset_result }.into_task_result(),
            )),
            Err(err) => match err {
                ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::BlockNotFound(_),
                ) => Ok(TaskOutcome::Failed(
                    TaskErrorDatasetReset::ResetHeadNotFound.into_task_error(),
                )),
                ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::CASFailed(e),
                ) => Ok(head_moved_outcome(task_reset.dataset_handle.id, Some(e))),
                ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::Access(e),
                ) => Ok(head_not_allowed_outcome(&e)),
                err @ ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::Internal(_),
                ) => {
                    tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "Reset failed",
                    );

                    Ok(TaskOutcome::Failed(TaskError::empty_recoverable()))
                }
            },
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
