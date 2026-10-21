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

use crate::{SecretSetSpecValidationError, SecretSetState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
pub enum SecretSetLifecycleError {
    #[error(transparent)]
    HeadersValidation(#[from] ResourceHeadersValidationError),

    #[error(transparent)]
    SpecValidation(#[from] SecretSetSpecValidationError),

    #[error("resource invariant violation: {0}")]
    InvariantViolation(Box<ProjectionError<SecretSetState>>),

    #[error(transparent)]
    StatusConflict(#[from] ResourceStatusConflictError),

    #[error(transparent)]
    DeletionInProgress(#[from] ResourceDeletionInProgressError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_resources::impl_resource_lifecycle_error!(SecretSetLifecycleError, SecretSetState);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
