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

use super::{InputSchemaNotDefinedError, InvalidInputIntervalError, TransformOptions};
use crate::engine::{EngineError, TransformRequestExt};
use crate::{
    CommitError,
    DataNotReproducible,
    EngineProvisioningError,
    ResolvedDataset,
    TransformListener,
    TransformPreliminaryPlan,
    TransformResult,
    VerificationListener,
    VerifyTransformOperation,
    WorkingDatasetsMap,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait TransformExecutionService: Send + Sync {
    async fn elaborate_transform(
        &self,
        target: ResolvedDataset,
        plan: TransformPreliminaryPlan,
        transform_options: &TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<TransformElaboration, TransformElaborateError>;

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

pub enum TransformElaboration {
    Elaborated(TransformPlan),
    UpToDate,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TransformPlan {
    pub request: TransformRequestExt,
    pub datasets_map: WorkingDatasetsMap,
}

impl std::fmt::Debug for TransformPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.request.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum TransformElaborateError {
    #[error(transparent)]
    InputSchemaNotDefined(
        #[from]
        #[backtrace]
        InputSchemaNotDefinedError,
    ),
    #[error(transparent)]
    InvalidInputInterval(
        #[from]
        #[backtrace]
        InvalidInputIntervalError,
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
        CommitError,
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
