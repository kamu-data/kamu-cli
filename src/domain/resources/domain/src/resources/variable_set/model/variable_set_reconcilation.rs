// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{
    DeclarativeResource,
    ReconcilableResource,
    ResourceReconcileError,
    VariableSetLifecycleError,
    VariableSetResource,
    VariableSetState,
    VariableSetStats,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ReconcilableResource for VariableSetResource {
    type ReconcileSuccess = VariableSetReconcileSuccess;
    type ReconcicleError = VariableSetReconcileError;
    type LifecycleError = VariableSetLifecycleError;
    type ResourceState = VariableSetState;

    fn needs_reconciliation(&self) -> bool {
        self.status()
            .resource_status
            .needs_reconciliation(self.metadata().generation)
    }

    fn try_mark_reconciliation_started(
        &mut self,
        now: DateTime<Utc>,
    ) -> Result<(), Self::LifecycleError> {
        // delegate to your existing aggregate method
        VariableSetResource::try_mark_reconciliation_started(self, now)
    }

    fn try_mark_reconciliation_succeeded(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        success: Self::ReconcileSuccess,
    ) -> Result<(), Self::LifecycleError> {
        VariableSetResource::try_mark_reconciliation_succeeded(
            self,
            now,
            expected_generation,
            success.stats,
        )
    }

    fn try_mark_reconciliation_failed(
        &mut self,
        now: DateTime<Utc>,
        expected_generation: u64,
        error: &Self::ReconcicleError,
    ) -> Result<(), Self::LifecycleError> {
        VariableSetResource::try_mark_reconciliation_failed(
            self,
            now,
            expected_generation,
            error.reason_code().to_string(),
            error.user_message(),
            VariableSetStats::pending_from_spec(self.spec()),
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
