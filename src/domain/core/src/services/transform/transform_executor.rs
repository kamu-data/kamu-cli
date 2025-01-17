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
use thiserror::Error;

use super::TransformPlan;
use crate::engine::EngineError;
use crate::{
    DataNotReproducible,
    EngineProvisioningError,
    ResolvedDataset,
    TransformListener,
    TransformResult,
    VerificationListener,
    VerifyTransformOperation,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformExecutor: Send + Sync {
    async fn execute_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPlan,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> (
        ResolvedDataset,
        Result<TransformResult, TransformExecuteError>,
    );

    async fn execute_verify_transform(
        &self,
        target: ResolvedDataset,
        verification_operation: VerifyTransformOperation,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> Result<(), VerifyTransformExecuteError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformExecuteError {
    #[error(transparent)]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),
    #[error(transparent)]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
    ),
    #[error(transparent)]
    CommitError(
        #[from]
        #[backtrace]
        odf::dataset::CommitError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VerifyTransformExecuteError {
    #[error(transparent)]
    EngineProvisioningError(
        #[from]
        #[backtrace]
        EngineProvisioningError,
    ),
    #[error(transparent)]
    EngineError(
        #[from]
        #[backtrace]
        EngineError,
    ),
    #[error("Data is not reproducible")]
    DataNotReproducible(
        #[from]
        #[backtrace]
        DataNotReproducible,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
