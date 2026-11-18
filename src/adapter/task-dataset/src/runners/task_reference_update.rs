// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system::{TaskError, TaskOutcome};

use crate::{DatasetRefMovedError, TaskErrorDatasetCommit};

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

/// The outcome of a task whose job lost the compare-and-swap of the target's
/// reference
pub(crate) fn ref_cas_failed_outcome(
    target: &odf::DatasetHandle,
    error: odf::dataset::RefCASError,
) -> TaskOutcome {
    ref_moved_outcome(target.id.clone(), error.reference.clone(), Some(error))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The outcome of a task whose principal may not run its operation, checked
/// before the operation or when it moves the dataset's reference: retrying
/// under the same principal cannot gain the access, so it is not retried
pub(crate) fn operation_not_allowed_outcome(error: &odf::AccessError) -> TaskOutcome {
    tracing::error!(error = ?error, error_msg = %error, "The task's principal is not allowed the operation");
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

/// Logs a domain failure of a task's job, as an error when retrying may help
/// and as a warning otherwise, and fails the task with an empty error of that
/// recoverability
pub(crate) fn failed_outcome(
    error: &impl std::error::Error,
    recoverable: bool,
    what: &str,
) -> TaskOutcome {
    if recoverable {
        tracing::error!(?error, error_msg = %error, "{what}");
    } else {
        tracing::warn!(?error, error_msg = %error, "{what}");
    }
    TaskOutcome::Failed(TaskError::empty(recoverable))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
