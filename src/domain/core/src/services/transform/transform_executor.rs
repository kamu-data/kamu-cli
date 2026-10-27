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
use kamu_datasets::ResolvedDataset;
use thiserror::Error;

use super::TransformExecutionPlan;
use crate::engine::EngineError;
use crate::{
    DataNotReproducible,
    EngineProvisioningError,
    IncompatibleSchemaError,
    TransformListener,
    TransformResult,
    VerificationListener,
    VerifyTransformOperation,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait TransformExecutor: Send + Sync {
    /// Runs the engine and appends the resulting blocks without moving
    /// `HEAD`. Needs no transaction: `target` and the datasets of `plan` are
    /// expected to be detached. The caller commits `HEAD` afterwards.
    ///
    /// Returns `UpToDate` when the engine produced nothing that would advance
    /// the dataset.
    ///
    /// `operation_id` identifies this execution attempt: it names the engine
    /// run directory and container, so every attempt needs its own.
    async fn execute_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformExecutionPlan,
        operation_id: &str,
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
    /// The engine's output does not fit the schema the dataset already has
    #[error(transparent)]
    IncompatibleSchema(
        #[from]
        #[backtrace]
        IncompatibleSchemaError,
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

impl TransformExecuteError {
    /// Whether executing the same plan again could succeed
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::EngineProvisioningError(e) => e.is_recoverable(),
            Self::EngineError(e) => e.is_recoverable(),
            Self::IncompatibleSchema(_) => false,
            Self::CommitError(_) | Self::Internal(_) => true,
        }
    }
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
