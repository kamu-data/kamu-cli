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
    ResourceExtensionResolutionError,
    ResourceExtensionValueError,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourceStatusConflictError,
    StatusConflictOf,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ResourceConditionUpdateError<R: EventSourcedResource> {
    /// Another writer changed the status since the caller read it
    #[error(transparent)]
    StatusGenerationChanged(ResourceStatusConflictError),

    #[error(transparent)]
    InvalidPatch(#[from] ResourceConditionPatchError),

    /// The type's status rules forbid the change
    #[error(transparent)]
    Rejected(#[from] ResourceStatusChangeRejectedError),

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

impl<R: EventSourcedResource> ResourceConditionUpdateError<R> {
    /// A refused status update; a status conflict is reported as such
    pub fn from_lifecycle_error(err: R::LifecycleError) -> Self {
        match err.as_status_conflict() {
            Some(conflict) => Self::StatusGenerationChanged(*conflict),
            None => Self::Lifecycle(err),
        }
    }
}

impl<R: EventSourcedResource> From<ResourcePersistenceError> for ResourceConditionUpdateError<R> {
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::Duplicate(err) => Self::Internal(
                err.int_err()
                    .with_context("Unexpected duplicate resource state in a condition update"),
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

/// A condition patch that no resource accepts
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ResourceConditionPatchError {
    #[error("condition '{key}' is written only by the resource framework")]
    FrameworkOwned { key: TypeUri },

    /// Unknown, not a condition, or not applicable to the resource type
    #[error(transparent)]
    Resolution(#[from] ResourceExtensionResolutionError),

    #[error("condition '{key}' value is invalid: {error}")]
    InvalidValue {
        key: TypeUri,
        error: ResourceExtensionValueError,
    },

    #[error("condition '{key}' takes {size} bytes, more than its limit of {max}")]
    TooLarge {
        key: TypeUri,
        size: usize,
        max: usize,
    },
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
#[error("status change rejected: {reason}")]
pub struct ResourceStatusChangeRejectedError {
    pub reason: String,
}

impl ResourceStatusChangeRejectedError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
