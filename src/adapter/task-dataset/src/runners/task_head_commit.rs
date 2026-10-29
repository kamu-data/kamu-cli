// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::HeadCommit;
use kamu_task_system::{TaskError, TaskOutcome};

use crate::{DatasetHeadMovedError, TaskErrorDatasetCommit};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task's `HEAD` commit as the task's outcome
pub(crate) trait HeadCommitTaskOutcome {
    /// `None` when the commit happened; otherwise the failed outcome of the
    /// task of `dataset_handle`
    fn into_failed_task_outcome(self, dataset_handle: &odf::DatasetHandle) -> Option<TaskOutcome>;
}

impl HeadCommitTaskOutcome for HeadCommit {
    fn into_failed_task_outcome(self, dataset_handle: &odf::DatasetHandle) -> Option<TaskOutcome> {
        match self {
            Self::Committed => None,
            Self::DatasetDeleted => Some(dataset_deleted_outcome(dataset_handle)),
            Self::HeadMoved(e) => Some(head_moved_outcome(dataset_handle.id.clone(), e)),
            // Retrying under the same account cannot gain the access
            Self::NotAllowed(e) => {
                tracing::error!(error = ?e, error_msg = %e, "Not allowed to commit the task's head");
                Some(TaskOutcome::Failed(TaskError::empty_unrecoverable()))
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a task whose `HEAD` was moved by another writer first
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
