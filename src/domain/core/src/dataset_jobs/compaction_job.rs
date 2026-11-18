// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::InternalError;
use kamu_datasets::DatasetPermissionError;
use thiserror::Error;

use crate::{
    CompactionExecutionError,
    CompactionListener,
    CompactionOptions,
    CompactionPlanningError,
    CompactionResult,
    map_set_chain_ref_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Compacts one dataset, or resets it to metadata, from start to end. It
/// authorizes the operation for the acting principal and plans in a short
/// transaction, merges with no transaction open, and moves `HEAD` in another
/// one; see `docs/internal/database-transactions.md`, "Who owns a transaction"
#[async_trait::async_trait]
pub trait CompactionJob: Send + Sync {
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactionError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Planning(
        #[from]
        #[backtrace]
        CompactionPlanningError,
    ),

    #[error(transparent)]
    Execution(
        #[from]
        #[backtrace]
        CompactionExecutionError,
    ),

    /// `HEAD` was moved concurrently; nothing was committed.
    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetPermissionError> for CompactionError {
    fn from(v: DatasetPermissionError) -> Self {
        v.into_error()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::SetChainRefError> for CompactionError {
    fn from(v: odf::dataset::SetChainRefError) -> Self {
        map_set_chain_ref_error(v)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
