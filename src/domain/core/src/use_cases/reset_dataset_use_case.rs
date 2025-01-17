// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;
use crate::{ResetExecutionError, ResetPlanningError, ResetResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResetDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetResult, ResetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ResetError {
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

impl From<DatasetActionUnauthorizedError> for ResetError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
