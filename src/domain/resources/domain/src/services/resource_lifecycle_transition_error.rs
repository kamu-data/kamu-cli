// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use event_sourcing::ConcurrentModificationError;
use internal_error::{ErrorIntoInternal, InternalError};

use crate::{
    EventSourcedResource,
    ResourceDeletionInProgressError,
    ResourceGenerationConflictError,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourceStatusConflictError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResourceLifecycleTransitionError<R: EventSourcedResource> {
    /// The resource changed since the caller read it, so the transition no
    /// longer applies
    #[error(transparent)]
    Stale(#[from] ResourceStaleExpectationError),

    #[error(transparent)]
    LoadFailed(#[from] ResourceLoadError<R::ResourceState>),

    /// Another writer saved the resource after it was loaded
    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Lifecycle(R::LifecycleError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl<R: EventSourcedResource> From<ResourcePersistenceError>
    for ResourceLifecycleTransitionError<R>
{
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::Duplicate(err) => Self::Internal(
                err.int_err()
                    .with_context("Unexpected duplicate resource state in a lifecycle transition"),
            ),
            ResourcePersistenceError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            ResourcePersistenceError::InvalidDurableState(err) => Self::Internal(err.int_err()),
            ResourcePersistenceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An expectation of the caller that no longer holds
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceStaleExpectationError {
    #[error(transparent)]
    DeletionRequested(ResourceDeletionInProgressError),

    #[error(transparent)]
    GenerationChanged(ResourceGenerationConflictError),

    #[error(transparent)]
    StatusGenerationChanged(ResourceStatusConflictError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
