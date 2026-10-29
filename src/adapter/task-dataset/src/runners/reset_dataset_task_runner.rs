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

use super::task_dataset_head_committer::TaskDatasetHeadCommitter;
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

impl ResetDatasetTaskRunner {
    #[tracing::instrument(level = "debug", skip_all, fields(?task_reset))]
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn run_reset(
        &self,
        task_reset: TaskDefinitionDatasetReset,
    ) -> Result<TaskOutcome, InternalError> {
        let Some(target) = dataset_registry
            .try_get_dataset_by_ref(&task_reset.dataset_handle.as_local_ref())
            .await?
        else {
            return Ok(TaskDatasetHeadCommitter::dataset_deleted_outcome(
                &task_reset.dataset_handle,
            ));
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
                ) => Ok(TaskDatasetHeadCommitter::head_moved_outcome(
                    task_reset.dataset_handle.id,
                    e,
                )),
                // Retrying under the same account cannot gain the access
                ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::Access(e),
                ) => {
                    tracing::error!(error = ?e, error_msg = %e, "Not allowed to reset the dataset");
                    Ok(TaskOutcome::Failed(TaskError::empty_unrecoverable()))
                }
                err @ (ResetExecutionError::SetReferenceFailed(
                    odf::dataset::SetChainRefError::Internal(_),
                )
                | ResetExecutionError::Internal(_)) => {
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
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<TaskOutcome, InternalError> {
        let task_reset = task_definition
            .downcast::<TaskDefinitionDatasetReset>()
            .expect("Mismatched task type for ResetDatasetTaskRunner");

        self.run_reset(*task_reset).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
