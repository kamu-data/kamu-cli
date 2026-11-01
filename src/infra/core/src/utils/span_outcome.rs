// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::{PullError, PullResponse, PullResult, SyncError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// How an update of a dataset ended, as the `outcome` field of its spans. Each
// span adds its own success words to these.
pub(crate) const UPDATED: &str = "updated";
pub(crate) const UP_TO_DATE: &str = "up_to_date";
pub(crate) const DATASET_DELETED: &str = "dataset_deleted";
pub(crate) const HEAD_MOVED: &str = "head_moved";
pub(crate) const NOT_ALLOWED: &str = "not_allowed";
pub(crate) const FAILED: &str = "failed";

/// Records `outcome` on the current span, which must declare the field
pub(crate) fn record_outcome(outcome: &'static str) {
    tracing::Span::current().record("outcome", outcome);
}

pub(crate) fn sync_error_outcome(error: &SyncError) -> &'static str {
    match error {
        SyncError::HeadMoved(_) => HEAD_MOVED,
        SyncError::Access(_) => NOT_ALLOWED,
        SyncError::DatasetNotFound(_)
        | SyncError::RefCollision(_)
        | SyncError::NameCollision(_)
        | SyncError::CreateDatasetFailed(_)
        | SyncError::UnsupportedProtocol(_)
        | SyncError::UnsupportedIpfsStorageType(_)
        | SyncError::RemoteByIdUnsupported(_)
        | SyncError::InvalidDestination(_)
        | SyncError::RepositoryNotFound(_)
        | SyncError::DatasetsDiverged(_)
        | SyncError::DatasetIdMismatch(_)
        | SyncError::DestinationAhead(_)
        | SyncError::OverwriteSeedBlock(_)
        | SyncError::Corrupted(_)
        | SyncError::Internal(_) => FAILED,
    }
}

/// Records how a pull job ended
pub(crate) fn record_pull_outcome(response: &Result<PullResponse, InternalError>) {
    record_outcome(match response {
        Ok(PullResponse { result, .. }) => match result {
            Ok(PullResult::Updated { .. }) => UPDATED,
            Ok(PullResult::UpToDate(_)) => UP_TO_DATE,
            Err(e) => pull_error_outcome(e),
        },
        Err(_) => FAILED,
    });
}

fn pull_error_outcome(error: &PullError) -> &'static str {
    match error {
        // A job reads its dataset by ID, so a missing one was deleted
        PullError::NotFound(_) => DATASET_DELETED,
        PullError::HeadMoved(_) => HEAD_MOVED,
        PullError::Access(_) => NOT_ALLOWED,
        PullError::SyncError(e) => sync_error_outcome(e),
        PullError::AmbiguousSource(_)
        | PullError::SaveUnderDifferentAlias(_)
        | PullError::AutoCreateDisabled
        | PullError::CannotInferLocalName(_)
        | PullError::ScanMetadata(_)
        | PullError::PollingIngestError(_)
        | PullError::IngestStopped(_)
        | PullError::TransformError(_)
        | PullError::TransformStopped(_)
        | PullError::Internal(_) => FAILED,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
