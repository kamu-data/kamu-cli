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
    TransformElaborateError,
    TransformExecuteError,
    TransformListener,
    TransformResult,
    map_set_chain_ref_error,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the next transform of one derivative. In a short transaction it
/// authorizes writing the derivative and reading its inputs for the acting
/// principal, and elaborates the transform at the current heads; the engine
/// runs with no transaction open, and `HEAD` moves with a compare-and-swap in
/// another one
#[async_trait::async_trait]
pub trait TransformJob: Send + Sync {
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformResult, TransformJobError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformJobError {
    /// The derivative or one of its inputs does not exist, or no longer does
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    /// The principal may not write the derivative, read an input or move
    /// `HEAD`
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Elaborate(
        #[from]
        #[backtrace]
        TransformElaborateError,
    ),

    #[error(transparent)]
    Execute(
        #[from]
        #[backtrace]
        TransformExecuteError,
    ),

    /// `HEAD` moved since elaboration; nothing was committed
    #[error(transparent)]
    RefCASFailed(#[from] odf::dataset::RefCASError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetPermissionError> for TransformJobError {
    fn from(v: DatasetPermissionError) -> Self {
        v.into_error()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::SetChainRefError> for TransformJobError {
    fn from(v: odf::dataset::SetChainRefError) -> Self {
        map_set_chain_ref_error(v)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
