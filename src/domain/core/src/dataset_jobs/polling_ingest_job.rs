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

use crate::{
    DataWriterMetadataState,
    PollingIngestError,
    PollingIngestListener,
    PollingIngestOptions,
    PollingIngestPlanError,
    PollingIngestResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one polling ingest iteration of one dataset. In a short transaction it
/// authorizes the ingest for the acting principal, plans it and resolves the
/// dataset's environment variables; it fetches and stages data with no
/// transaction open, and moves `HEAD` with a compare-and-swap in another one.
/// `kamu pull` repeats it while the source has more data; the update tasks of
/// flows run it once
#[async_trait::async_trait]
pub trait PollingIngestJob: Send + Sync {
    /// `maybe_continuation` is the writer state the previous iteration of the
    /// same ingest ended with; the iteration starts from it instead of
    /// scanning the metadata again
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        options: PollingIngestOptions,
        maybe_continuation: Option<Box<DataWriterMetadataState>>,
        maybe_listener: Option<Arc<dyn PollingIngestListener>>,
    ) -> Result<PollingIngestResult, PollingIngestJobError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum PollingIngestJobError {
    /// The dataset does not exist, or no longer does
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    /// The principal may not ingest into the dataset or move its `HEAD`
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Planning(
        #[from]
        #[backtrace]
        PollingIngestPlanError,
    ),

    #[error(transparent)]
    Ingest(
        #[from]
        #[backtrace]
        PollingIngestError,
    ),

    /// `HEAD` moved since planning; nothing was committed
    #[error(transparent)]
    RefMoved(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetPermissionError> for PollingIngestJobError {
    fn from(v: DatasetPermissionError) -> Self {
        use DatasetPermissionError as E;

        match v {
            E::NotFound(e) => Self::NotFound(e),
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
