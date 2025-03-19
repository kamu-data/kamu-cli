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
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;
use crate::{
    CompactionExecutionError,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionPlanningError,
    CompactionResult,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait CompactDatasetUseCase: Send + Sync {
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError>;

    async fn execute_multi(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        options: CompactionOptions,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Vec<CompactionResponse>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CompactionResponse {
    pub dataset_ref: odf::DatasetRef,
    pub result: Result<CompactionResult, CompactionError>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CompactionError {
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

impl From<DatasetActionUnauthorizedError> for CompactionError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::dataset::SetChainRefError> for CompactionError {
    fn from(v: odf::dataset::SetChainRefError) -> Self {
        match v {
            odf::dataset::SetChainRefError::Access(e) => Self::Access(e),
            odf::dataset::SetChainRefError::Internal(e) => Self::Internal(e),
            _ => Self::Internal(v.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
