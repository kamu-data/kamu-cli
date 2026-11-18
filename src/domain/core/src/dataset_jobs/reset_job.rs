// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_datasets::DatasetPermissionError;

use crate::{ResetExecutionError, ResetPlanningError, ResetResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Resets one dataset to an earlier block in one short transaction: it
/// authorizes the reset for the acting principal, checks the expected old
/// head and moves `HEAD` with a compare-and-swap
#[async_trait::async_trait]
pub trait ResetJob: Send + Sync {
    async fn run(
        &self,
        target: &odf::DatasetHandle,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetResult, ResetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResetError {
    #[error(transparent)]
    NotFound(#[from] odf::DatasetNotFoundError),

    #[error(transparent)]
    Planning(
        #[from]
        #[backtrace]
        ResetPlanningError,
    ),

    #[error(transparent)]
    Execution(
        #[from]
        #[backtrace]
        ResetExecutionError,
    ),

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

impl From<DatasetPermissionError> for ResetError {
    fn from(v: DatasetPermissionError) -> Self {
        v.into_error()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
