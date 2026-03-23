// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ReconcileFailureMapper,
    ResourceReconcileError,
    SecretSetFailureDetails,
    SecretSetLifecycleError,
    SecretSetResource,
    SecretSetStats,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for SecretSetResource {
    type ReconcileSuccess = SecretSetReconcileSuccess;
    type ReconcileError = SecretSetReconcileError;
    type LifecycleError = SecretSetLifecycleError;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableEventSourcedResource for SecretSetResource {
    type FailureDetails = SecretSetFailureDetails;

    fn apply_event(
        &mut self,
        event: crate::ReconcilableResourceEvent<
            Self::Spec,
            Self::ReconcileSuccess,
            Self::FailureDetails,
        >,
    ) -> Result<(), Self::LifecycleError> {
        self.apply(event)
            .map_err(|e| SecretSetLifecycleError::InvariantViolation(Box::new(e)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcileFailureMapper for SecretSetResource {
    fn failure_details(_error: &Self::ReconcileError) -> Self::FailureDetails {
        SecretSetFailureDetails {
            stats: SecretSetStats::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct SecretSetReconcileSuccess {
    pub stats: SecretSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum SecretSetReconcileError {
    #[error("Reference missing: {name}")]
    ReferenceMissing { name: String },

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ResourceReconcileError for SecretSetReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            SecretSetReconcileError::ReferenceMissing { .. } => "reference_missing",
            SecretSetReconcileError::Internal(_) => "internal_error",
        }
    }

    fn user_message(&self) -> String {
        match self {
            SecretSetReconcileError::ReferenceMissing { name } => {
                format!("Referenced resource '{name}' is missing.")
            }
            SecretSetReconcileError::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            SecretSetReconcileError::ReferenceMissing { .. } => false,
            SecretSetReconcileError::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
