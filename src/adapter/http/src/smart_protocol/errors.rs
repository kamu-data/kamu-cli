// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{BoxedError, ErrorIntoInternal, InternalError};
use kamu_core::{OverwriteSeedBlockError, SyncError, SyncRefMovedError};
use kamu_datasets::NameCollisionError;
use reqwest::StatusCode;
use thiserror::Error;

use super::messages::{ObjectFileReference, TransferInternalError};
use super::phases::*;
use crate::ws_common::{ReadMessageError, WriteMessageError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reading a message from the socket failed during a transfer phase
#[derive(Error, Debug)]
#[error("Smart protocol read failed: phase={phase}, error={read_error}")]
pub struct PhaseReadError {
    pub read_error: ReadMessageError,
    pub phase: TransferPhase,
}

impl PhaseReadError {
    pub fn new(read_error: ReadMessageError, phase: impl Into<TransferPhase>) -> Self {
        Self {
            read_error,
            phase: phase.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Writing a message to the socket failed during a transfer phase
#[derive(Error, Debug)]
#[error("Smart protocol write failed: phase={phase}, error={write_error}")]
pub struct PhaseWriteError {
    pub write_error: WriteMessageError,
    pub phase: TransferPhase,
}

impl PhaseWriteError {
    pub fn new(write_error: WriteMessageError, phase: impl Into<TransferPhase>) -> Self {
        Self {
            write_error,
            phase: phase.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PullServerError {
    #[error(transparent)]
    ReadFailed(#[from] PhaseReadError),

    #[error(transparent)]
    WriteFailed(#[from] PhaseWriteError),

    #[error(transparent)]
    Internal(PhaseInternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PullClientError {
    #[error("Overwriting dataset id or kind is restricted")]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(#[from] PhaseReadError),

    #[error(transparent)]
    WriteFailed(#[from] PhaseWriteError),

    #[error(transparent)]
    InvalidInterval(odf::dataset::InvalidIntervalError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PushServerError {
    #[error(transparent)]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(#[from] PhaseReadError),

    #[error(transparent)]
    WriteFailed(#[from] PhaseWriteError),

    #[error(transparent)]
    RefCollision(odf::dataset::RefCollisionError),

    #[error(transparent)]
    NameCollision(NameCollisionError),

    /// `HEAD` differs from the one the client planned the push from; nothing
    /// was transferred
    #[error(transparent)]
    InvalidHead(odf::dataset::RefCASError),

    /// `HEAD` moved while the pushed blocks were being committed
    #[error(transparent)]
    UpdatedConcurrently(odf::dataset::RefCASError),

    /// The client completed the push without uploading every file the pushed
    /// blocks reference; nothing was committed
    #[error(transparent)]
    ObjectsNotUploaded(ObjectsNotUploadedError),

    #[error(transparent)]
    Internal(PhaseInternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error(
    "Client did not upload {num_missing} file(s) the pushed blocks reference, such as {:?} {}",
    first_missing.object_type,
    first_missing.physical_hash
)]
pub struct ObjectsNotUploadedError {
    pub num_missing: usize,
    pub first_missing: ObjectFileReference,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum PushClientError {
    #[error(transparent)]
    OverwriteSeedBlock(OverwriteSeedBlockError),

    #[error(transparent)]
    ReadFailed(#[from] PhaseReadError),

    #[error(transparent)]
    WriteFailed(#[from] PhaseWriteError),

    #[error(transparent)]
    InvalidHead(odf::dataset::RefCASError),

    #[error(transparent)]
    RefCollision(odf::dataset::RefCollisionError),

    #[error(transparent)]
    NameCollision(NameCollisionError),

    #[error(transparent)]
    UpdatedConcurrently(DestinationUpdatedConcurrentlyError),

    #[error(transparent)]
    ObjectsNotUploaded(DestinationMissingObjectsError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<PushClientError> for SyncError {
    fn from(e: PushClientError) -> Self {
        match e {
            PushClientError::OverwriteSeedBlock(e) => Self::OverwriteSeedBlock(e),
            PushClientError::RefCollision(e) => Self::RefCollision(e),
            PushClientError::NameCollision(e) => Self::NameCollision(e),
            PushClientError::InvalidHead(e) => Self::RefMoved(e.into()),
            PushClientError::UpdatedConcurrently(_) => {
                Self::RefMoved(SyncRefMovedError::unreported(odf::BlockRef::Head))
            }
            e @ (PushClientError::ReadFailed(_)
            | PushClientError::WriteFailed(_)
            | PushClientError::ObjectsNotUploaded(_)
            | PushClientError::Internal(_)) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The destination `HEAD` moved while the pushed blocks were being committed;
/// nothing was committed
#[derive(Error, Debug)]
#[error("Destination dataset head was updated concurrently, nothing was committed")]
pub struct DestinationUpdatedConcurrentlyError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The destination refused to commit the pushed blocks, because some files
/// they reference were not uploaded; nothing was committed
#[derive(Error, Debug)]
#[error(
    "Destination refused the commit: {num_objects} file(s) the pushed blocks reference were not \
     uploaded"
)]
pub struct DestinationMissingObjectsError {
    pub num_objects: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("{details} during phase: {phase}")]
pub struct ClientInternalError {
    phase: TransferPhase,
    details: String,
}

impl From<TransferInternalError> for ClientInternalError {
    fn from(e: TransferInternalError) -> Self {
        Self {
            phase: e.phase,
            details: e.error_message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Object download failed with status {status}")]
pub struct ObjectDownloadError {
    pub status: StatusCode,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Object upload failed with status {status}")]
pub struct ObjectUploadError {
    pub status: StatusCode,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Smart protocol phase internal error: phase={phase}, error={error}")]
pub struct PhaseInternalError {
    pub phase: TransferPhase,
    pub error: InternalError,
}

impl From<PhaseInternalError> for PushServerError {
    fn from(value: PhaseInternalError) -> Self {
        PushServerError::Internal(value)
    }
}

impl From<PhaseInternalError> for PullServerError {
    fn from(value: PhaseInternalError) -> Self {
        PullServerError::Internal(value)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ErrorIntoProtocolInternal {
    fn protocol_int_err<P>(self, phase: P) -> PhaseInternalError
    where
        P: Into<TransferPhase>;
}

impl<E> ErrorIntoProtocolInternal for E
where
    E: Into<BoxedError>,
{
    fn protocol_int_err<P>(self, phase: P) -> PhaseInternalError
    where
        P: Into<TransferPhase>,
    {
        PhaseInternalError {
            phase: phase.into(),
            error: InternalError::new(self),
        }
    }
}

pub trait ResultIntoProtocolInternal<OK> {
    fn protocol_int_err<P>(self, phase: P) -> Result<OK, PhaseInternalError>
    where
        P: Into<TransferPhase>;
}

impl<OK, E> ResultIntoProtocolInternal<OK> for Result<OK, E>
where
    E: Into<BoxedError>,
{
    fn protocol_int_err<P>(self, phase: P) -> Result<OK, PhaseInternalError>
    where
        P: Into<TransferPhase>,
    {
        match self {
            Ok(ok) => Ok(ok),
            Err(e) => Err(e.protocol_int_err(phase)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use std::assert_matches;

    use kamu_core::{SyncError, SyncRefMovedError};

    use super::{DestinationUpdatedConcurrentlyError, PushClientError};

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn test_stale_push_head_is_a_recoverable_head_move_with_heads() {
        let fixtures = PushErrorFixtures;

        let sync_error = SyncError::from(PushClientError::InvalidHead(fixtures.stale_head_error()));

        assert!(sync_error.is_recoverable());
        assert_matches!(
            sync_error,
            SyncError::RefMoved(SyncRefMovedError { reported: Some(heads), .. })
                if heads == fixtures.stale_head_error()
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn test_ref_moved_during_push_commit_is_recoverable_without_block_hashes() {
        let sync_error = SyncError::from(PushClientError::UpdatedConcurrently(
            DestinationUpdatedConcurrentlyError,
        ));

        assert!(sync_error.is_recoverable());
        assert_matches!(
            sync_error,
            SyncError::RefMoved(SyncRefMovedError { reported: None, .. })
        );
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    struct PushErrorFixtures;

    impl PushErrorFixtures {
        fn stale_head_error(&self) -> odf::dataset::RefCASError {
            odf::dataset::RefCASError {
                reference: odf::BlockRef::Head,
                expected: Some(odf::Multihash::from_digest_sha3_256(b"planned")),
                actual: Some(odf::Multihash::from_digest_sha3_256(b"concurrent")),
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
