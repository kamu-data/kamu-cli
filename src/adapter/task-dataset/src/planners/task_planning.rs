// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{DatasetRegistry, DatasetRegistryExt, ResolvedDataset};
use kamu_task_system::{PrepareTaskDefinitionError, TaskError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resolves the dataset a task targets. A dataset deleted after the task was
/// scheduled fails the task unrecoverably: nothing is left to plan.
pub(crate) async fn resolve_task_dataset(
    dataset_registry: &dyn DatasetRegistry,
    dataset_id: &odf::DatasetID,
) -> Result<ResolvedDataset, PrepareTaskDefinitionError> {
    match dataset_registry.get_dataset_by_id(dataset_id).await {
        Ok(target) => Ok(target),
        Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
            tracing::warn!(error = %e, "The task's dataset no longer exists");
            Err(PrepareTaskDefinitionError::Failed(
                TaskError::empty_unrecoverable(),
            ))
        }
        Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e.into()),
    }
}

/// Logs a domain planning error and fails the task with an empty error of the
/// given recoverability
pub(crate) fn planning_failed(
    error: &impl std::error::Error,
    recoverable: bool,
) -> PrepareTaskDefinitionError {
    if recoverable {
        tracing::error!(?error, error_msg = %error, "Task planning failed");
    } else {
        tracing::warn!(?error, error_msg = %error, "Task planning failed");
    }
    PrepareTaskDefinitionError::Failed(TaskError::empty(recoverable))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
