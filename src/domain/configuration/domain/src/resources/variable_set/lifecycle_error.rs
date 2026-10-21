// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ProjectionError;
use kamu_resources::{
    ResourceDeletionInProgressError,
    ResourceHeadersValidationError,
    ResourceStatusConflictError,
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

    #[error(transparent)]
    StatusConflict(#[from] ResourceStatusConflictError),

    #[error(transparent)]
    DeletionInProgress(#[from] ResourceDeletionInProgressError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::impl_resource_lifecycle_error!(VariableSetLifecycleError, VariableSetState);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
