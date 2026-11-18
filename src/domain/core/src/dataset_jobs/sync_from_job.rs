// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_datasets::DatasetPermissionError;
use thiserror::Error;

use crate::{PullLocalTarget, SyncError, SyncListener, SyncOptions, SyncResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Pulls a remote dataset into a local one. In a short transaction it
/// authorizes writing an existing target for the acting principal and reads
/// the head the commit will expect; it transfers blocks and data with no
/// transaction open, and commits in another one: a compare-and-swap of
/// `HEAD`, or the creation of a new dataset. `kamu pull` and the update tasks
/// of flows run it
#[async_trait::async_trait]
pub trait SyncFromJob: Send + Sync {
    async fn run(
        &self,
        target: &PullLocalTarget,
        src: &odf::DatasetRefRemote,
        options: SyncOptions,
        maybe_listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<SyncResult, SyncFromJobError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum SyncFromJobError {
    /// The existing target was deleted before the commit
    #[error(transparent)]
    TargetDeleted(odf::DatasetNotFoundError),

    #[error(transparent)]
    Sync(
        #[from]
        #[backtrace]
        SyncError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetPermissionError> for SyncFromJobError {
    fn from(v: DatasetPermissionError) -> Self {
        use DatasetPermissionError as E;

        match v {
            E::NotFound(e) => Self::TargetDeleted(e),
            E::Access(e) => Self::Sync(SyncError::Access(e)),
            e @ E::Internal(_) => Self::Sync(SyncError::Internal(e.int_err())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetNotFoundError> for SyncFromJobError {
    fn from(v: odf::DatasetNotFoundError) -> Self {
        Self::TargetDeleted(v)
    }
}

impl From<InternalError> for SyncFromJobError {
    fn from(v: InternalError) -> Self {
        Self::Sync(SyncError::Internal(v))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
