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
    type FailureDetails = SecretSetFailureDetails;
    type LifecycleError = SecretSetLifecycleError;

    fn failure_details(_error: &Self::ReconcileError) -> Self::FailureDetails {
        SecretSetFailureDetails {
            stats: SecretSetStats::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableEventSourcedResource for SecretSetResource {}

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
