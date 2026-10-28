// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::{DatasetUpdateSteps, HeadCommit};
use kamu_task_system::{TaskError, TaskOutcome};

use crate::{DatasetHeadMovedError, TaskErrorDatasetCommit};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Commits a `HEAD` proposed by a task, translating a commit that did not
/// happen into the task's failed outcome.
pub(crate) struct TaskDatasetHeadCommitter<'a> {
    update_steps: &'a dyn DatasetUpdateSteps,
}

impl<'a> TaskDatasetHeadCommitter<'a> {
    pub(crate) fn new(update_steps: &'a dyn DatasetUpdateSteps) -> Self {
        Self { update_steps }
    }

    /// Moves `HEAD` from `old_head` to `new_head` in a transaction of its own.
    /// Returns the failed outcome when `HEAD` was moved concurrently, the
    /// commit was not allowed or the dataset was deleted.
    pub(crate) async fn commit(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<Option<TaskOutcome>, InternalError> {
        match self
            .update_steps
            .commit_head(dataset_handle, old_head, new_head)
            .await?
        {
            HeadCommit::Committed => Ok(None),
            HeadCommit::DatasetDeleted => Ok(Some(Self::dataset_deleted_outcome(dataset_handle))),
            HeadCommit::HeadMoved(e) => {
                Ok(Some(Self::head_moved_outcome(dataset_handle.id.clone(), e)))
            }
            // Retrying under the same account cannot gain the access
            HeadCommit::NotAllowed(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Not allowed to commit the task's head");
                Ok(Some(TaskOutcome::Failed(TaskError::empty_unrecoverable())))
            }
        }
    }

    pub(crate) fn head_moved_outcome(
        dataset_id: odf::DatasetID,
        cas_error: odf::dataset::RefCASError,
    ) -> TaskOutcome {
        TaskOutcome::Failed(
            TaskErrorDatasetCommit::HeadMoved(DatasetHeadMovedError::new(dataset_id, cas_error))
                .into_task_error(),
        )
    }

    /// The outcome of a task whose dataset was deleted after the task was
    /// scheduled: nothing is left to update, so it is not retried
    pub(crate) fn dataset_deleted_outcome(dataset_handle: &odf::DatasetHandle) -> TaskOutcome {
        tracing::warn!(%dataset_handle, "The task's dataset no longer exists");
        TaskOutcome::Failed(TaskError::empty_unrecoverable())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
