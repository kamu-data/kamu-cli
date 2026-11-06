// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_core::{HeadCommit, PullError, PullResponse, PullResult, SyncError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How a step of a dataset update ended, recorded as the `outcome` field of
/// its span
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub(crate) enum SpanOutcome {
    Updated,
    UpToDate,
    DatasetDeleted,
    HeadMoved,
    NotAllowed,
    Failed,
    Planned,
    Elaborated,
    Existing,
    New,
    Committed,
}

impl SpanOutcome {
    /// Records this outcome on the current span, which must declare the field
    pub(crate) fn record(self) {
        let outcome: &'static str = self.into();
        tracing::Span::current().record("outcome", outcome);
    }

    /// How a pull job ended
    pub(crate) fn of_pull_response(response: &Result<PullResponse, InternalError>) -> Self {
        match response {
            Ok(PullResponse { result, .. }) => match result {
                Ok(PullResult::Updated { .. }) => Self::Updated,
                Ok(PullResult::UpToDate(_)) => Self::UpToDate,
                Err(e) => e.into(),
            },
            Err(_) => Self::Failed,
        }
    }
}

impl From<&SyncError> for SpanOutcome {
    fn from(error: &SyncError) -> Self {
        match error {
            SyncError::HeadMoved(_) => Self::HeadMoved,
            SyncError::Access(_) => Self::NotAllowed,
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
            | SyncError::Internal(_) => Self::Failed,
        }
    }
}

impl From<&PullError> for SpanOutcome {
    fn from(error: &PullError) -> Self {
        match error {
            // A job reads its dataset by ID, so a missing one was deleted
            PullError::NotFound(_) => Self::DatasetDeleted,
            PullError::HeadMoved(_) => Self::HeadMoved,
            PullError::Access(_) => Self::NotAllowed,
            PullError::SyncError(e) => e.into(),
            PullError::AmbiguousSource(_)
            | PullError::SaveUnderDifferentAlias(_)
            | PullError::AutoCreateDisabled
            | PullError::CannotInferLocalName(_)
            | PullError::ScanMetadata(_)
            | PullError::PollingIngestError(_)
            | PullError::IngestStopped(_)
            | PullError::TransformError(_)
            | PullError::TransformStopped(_)
            | PullError::Internal(_) => Self::Failed,
        }
    }
}

impl From<&HeadCommit> for SpanOutcome {
    fn from(head_commit: &HeadCommit) -> Self {
        match head_commit {
            HeadCommit::Committed => Self::Committed,
            HeadCommit::HeadMoved(_) => Self::HeadMoved,
            HeadCommit::DatasetDeleted => Self::DatasetDeleted,
            HeadCommit::NotAllowed(_) => Self::NotAllowed,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use strum::IntoEnumIterator;

    use super::*;

    #[test]
    fn test_outcome_names_are_stable() {
        let names = [
            (SpanOutcome::Updated, "updated"),
            (SpanOutcome::UpToDate, "up_to_date"),
            (SpanOutcome::DatasetDeleted, "dataset_deleted"),
            (SpanOutcome::HeadMoved, "head_moved"),
            (SpanOutcome::NotAllowed, "not_allowed"),
            (SpanOutcome::Failed, "failed"),
            (SpanOutcome::Planned, "planned"),
            (SpanOutcome::Elaborated, "elaborated"),
            (SpanOutcome::Existing, "existing"),
            (SpanOutcome::New, "new"),
            (SpanOutcome::Committed, "committed"),
        ];

        assert_eq!(names.len(), SpanOutcome::iter().count());
        for (outcome, name) in names {
            assert_eq!(<&'static str>::from(outcome), name);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
