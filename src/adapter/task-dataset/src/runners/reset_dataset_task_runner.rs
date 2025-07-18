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
use kamu_core::{DatasetRegistry, ResetExecutionError, ResetExecutor};
use kamu_task_system::*;

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
        let target = dataset_registry
            .get_dataset_by_handle(&task_reset.dataset_handle)
            .await;

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
                err => {
                    tracing::error!(
                        error = ?err,
                        error_msg = %err,
                        "Reset failed",
                    );

                    Ok(TaskOutcome::Failed(TaskError::empty()))
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
