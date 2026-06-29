// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ProjectionError;
use internal_error::ErrorIntoInternal;
use kamu_resources::{
    ApplyResourceLifecycleErrorHandling,
    ApplyResourceRejection,
    ApplyResourceRejectionCategory,
    IntoApplyResourceRejection,
    ResourceHeadersValidationError,
};
use thiserror::Error;

use crate::{VariableSetSpecValidationError, VariableSetState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum VariableSetLifecycleError {
    #[error(transparent)]
    HeadersValidation(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    SpecValidation(#[from] VariableSetSpecValidationError),

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<VariableSetState>>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::impl_invariant_violation_lifecycle_error!(
    VariableSetLifecycleError,
    VariableSetState
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl IntoApplyResourceRejection for VariableSetLifecycleError {
    fn into_apply_resource_rejection(self) -> ApplyResourceLifecycleErrorHandling {
        match self {
            Self::HeadersValidation(err) => {
                ApplyResourceLifecycleErrorHandling::Rejected(ApplyResourceRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    message: err.to_string(),
                })
            }
            Self::SpecValidation(err) => {
                ApplyResourceLifecycleErrorHandling::Rejected(ApplyResourceRejection {
                    category: ApplyResourceRejectionCategory::BusinessValidationFailed,
                    message: err.to_string(),
                })
            }
            Self::InvariantViolation(err) => {
                ApplyResourceLifecycleErrorHandling::Technical(err.int_err())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
