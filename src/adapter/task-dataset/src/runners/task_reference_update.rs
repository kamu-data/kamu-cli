// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::ReferenceUpdate;
use kamu_task_system::{TaskError, TaskOutcome};

use crate::{DatasetRefMovedError, TaskErrorDatasetCommit};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A task's reference update as the task's outcome
pub(crate) trait ReferenceUpdateTaskOutcome {
    /// `None` when the update was committed; otherwise the failed outcome of the
    /// task of `dataset_handle`
    fn into_failed_task_outcome(self, dataset_handle: &odf::DatasetHandle) -> Option<TaskOutcome>;
}

impl ReferenceUpdateTaskOutcome for ReferenceUpdate {
    fn into_failed_task_outcome(self, dataset_handle: &odf::DatasetHandle) -> Option<TaskOutcome> {
        match self {
            Self::Committed => None,
            Self::DatasetDeleted => Some(dataset_deleted_outcome(dataset_handle)),
            Self::RefMoved(e) => Some(ref_moved_outcome(
                dataset_handle.id.clone(),
                e.reference.clone(),
                Some(e),
            )),
            Self::NotAllowed(e) => Some(ref_update_not_allowed_outcome(&e)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a task whose reference was moved by another writer first;
/// `reported` is `None` when the destination did not report the block hashes
pub(crate) fn ref_moved_outcome(
    dataset_id: odf::DatasetID,
    block_ref: odf::BlockRef,
    reported: Option<odf::dataset::RefCASError>,
) -> TaskOutcome {
    let error = DatasetRefMovedError::new(dataset_id, block_ref, reported);
    tracing::warn!(
        dataset_id = %error.dataset_id,
        block_ref = %error.block_ref,
        expected_block_hash = ?error.expected_block_hash,
        actual_block_hash = ?error.actual_block_hash,
        "The task's dataset reference was moved concurrently"
    );
    TaskOutcome::Failed(TaskErrorDatasetCommit::RefMoved(error).into_task_error())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a task that may not move its dataset's reference: retrying
/// under the same account cannot gain the access, so it is not retried
pub(crate) fn ref_update_not_allowed_outcome(error: &odf::AccessError) -> TaskOutcome {
    tracing::error!(error = ?error, error_msg = %error, "Not allowed to move the task's dataset reference");
    TaskOutcome::Failed(TaskError::empty_unrecoverable())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a task whose dataset was deleted after the task was
/// scheduled: nothing is left to update, so it is not retried
pub(crate) fn dataset_deleted_outcome(dataset_handle: &odf::DatasetHandle) -> TaskOutcome {
    tracing::warn!(%dataset_handle, "The task's dataset no longer exists");
    TaskOutcome::Failed(TaskError::empty_unrecoverable())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
