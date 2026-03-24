// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use crate::{
    ReconcilableEventSourcedResource,
    ReconcilableResource,
    ResourceReconcileError,
    VariableSetFailureDetails,
    VariableSetLifecycleError,
    VariableSetResource,
    VariableSetStats,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for VariableSetResource {
    type ReconcileSuccess = VariableSetReconcileSuccess;
    type ReconcileError = VariableSetReconcileError;
    type FailureDetails = VariableSetFailureDetails;
    type LifecycleError = VariableSetLifecycleError;

    fn failure_details(_error: &Self::ReconcileError) -> Self::FailureDetails {
        VariableSetFailureDetails {
            stats: VariableSetStats::default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableEventSourcedResource for VariableSetResource {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariableSetReconcileSuccess {
    pub stats: VariableSetStats,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetReconcileError {
    #[error("Reference missing: {name}")]
    ReferenceMissing { name: String },

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ResourceReconcileError for VariableSetReconcileError {
    fn reason_code(&self) -> &'static str {
        match self {
            VariableSetReconcileError::ReferenceMissing { .. } => "reference_missing",
            VariableSetReconcileError::Internal(_) => "internal_error",
        }
    }

    fn user_message(&self) -> String {
        match self {
            VariableSetReconcileError::ReferenceMissing { name } => {
                format!("Referenced resource '{name}' is missing.")
            }
            VariableSetReconcileError::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            VariableSetReconcileError::ReferenceMissing { .. } => false,
            VariableSetReconcileError::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
