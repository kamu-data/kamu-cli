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
    /// Runs the engine and appends the resulting blocks without moving `HEAD`;
    /// `UpToDate` when nothing would advance the dataset. Needs no
    /// transaction: `target` and the datasets of `plan` are detached. The
    /// listener gets `begin` and engine progress; the caller commits `HEAD`
    /// and reports the outcome. `operation_id` is unique per attempt, as
    /// [`crate::engine::Engine::execute_transform`] requires.
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

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use internal_error::ErrorIntoInternal;

    use super::*;
    use crate::UnsupportedEngineError;
    use crate::engine::ProcessError;

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    #[test]
    fn test_transform_execute_error_recoverability() {
        let cases = [
            (
                TransformExecuteError::EngineProvisioningError(
                    UnsupportedEngineError {
                        engine_id: "cobol".to_string(),
                    }
                    .into(),
                ),
                false,
            ),
            (
                TransformExecuteError::EngineProvisioningError(EngineProvisioningError::Internal(
                    "provisioning failed".int_err(),
                )),
                true,
            ),
            (
                TransformExecuteError::EngineError(EngineError::invalid_query(
                    "unknown column",
                    Vec::new(),
                )),
                false,
            ),
            (
                TransformExecuteError::EngineError(ProcessError::new(Some(1), Vec::new()).into()),
                true,
            ),
            (
                TransformExecuteError::IncompatibleSchema(IncompatibleSchemaError::new(
                    "Schema of the new slice is not compatible",
                    Arc::new(Schema::empty()),
                    Arc::new(Schema::empty()),
                )),
                false,
            ),
            (
                TransformExecuteError::CommitError(odf::dataset::CommitError::Internal(
                    "storage unavailable".int_err(),
                )),
                true,
            ),
            (
                TransformExecuteError::Internal("engine container exited".int_err()),
                true,
            ),
        ];

        for (error, recoverable) in cases {
            assert_eq!(error.is_recoverable(), recoverable, "{error:?}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
