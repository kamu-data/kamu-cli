// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use kamu_resources::{
    RECONCILIATION_ERROR_CODE_CONCURRENT_MODIFICATION,
    RECONCILIATION_ERROR_CODE_INTERNAL_ERROR,
    ResourceReconcileError,
};

use crate::{VariableSetLifecycleError, VariableSetResource};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::impl_event_sourced_resource!(
    resource = VariableSetResource,
    reconcile_error = VariableSetReconcileError,
    lifecycle_error = VariableSetLifecycleError,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum VariableSetReconcileError {
    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        internal_error::InternalError,
    ),
}

impl ResourceReconcileError for VariableSetReconcileError {
    fn code(&self) -> &'static str {
        match self {
            VariableSetReconcileError::ConcurrentModification(_) => {
                RECONCILIATION_ERROR_CODE_CONCURRENT_MODIFICATION
            }
            VariableSetReconcileError::Internal(_) => RECONCILIATION_ERROR_CODE_INTERNAL_ERROR,
        }
    }

    fn user_message(&self) -> String {
        match self {
            VariableSetReconcileError::ConcurrentModification(_) => {
                "Resource was modified concurrently during reconciliation.".to_owned()
            }
            VariableSetReconcileError::Internal(e) => format!("Internal error: {e}"),
        }
    }

    fn is_transient(&self) -> bool {
        match self {
            VariableSetReconcileError::ConcurrentModification(_)
            | VariableSetReconcileError::Internal(_) => true,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
