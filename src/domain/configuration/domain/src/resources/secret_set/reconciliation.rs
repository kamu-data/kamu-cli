// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_resources::ResourceReconcileError;
use serde::{Deserialize, Serialize};

use crate::{SecretSetFailureDetails, SecretSetLifecycleError, SecretSetResource, SecretSetStats};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::impl_reconcilable_event_sourced_resource!(
    resource = SecretSetResource,
    reconcile_success = SecretSetReconcileSuccess,
    reconcile_error = SecretSetReconcileError,
    reconcile_failure_details = SecretSetFailureDetails,
    lifecycle_error = SecretSetLifecycleError,
    reconcile_failure_details_fn = |_error| {
        SecretSetFailureDetails {
            stats: SecretSetStats::default(),
        }
    }
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
