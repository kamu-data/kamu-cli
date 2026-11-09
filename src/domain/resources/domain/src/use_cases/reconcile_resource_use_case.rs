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
    ResourceID,
    ResourceLifecycleTransitionError,
    ResourceLoadError,
    ResourcePersistenceError,
    ResourceStaleExpectationError,
    ResourceStatusConflictError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait ReconcileResourceUseCase<R: EventSourcedResource>: Send + Sync {
    async fn execute(&self, id: &ResourceID) -> Result<(), ReconcileResourceUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ReconcileResourceUseCaseError<R: EventSourcedResource> {
    #[error(transparent)]
    LoadFailed(#[from] ResourceLoadError<R::ResourceState>),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    /// Another writer updated the status after reconciliation started.
    #[error(transparent)]
    StatusConflict(ResourceStatusConflictError),

    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    Lifecycle(R::LifecycleError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ResourcePersistenceError> for ReconcileResourceUseCaseError<R>
where
    R: EventSourcedResource,
{
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::Duplicate(err) => Self::Internal(err.int_err().with_context(
                "Unexpected duplicate resource state while reconciling existing resource",
            )),
            ResourcePersistenceError::ConcurrentModification(err) => {
                Self::ConcurrentModification(err)
            }
            ResourcePersistenceError::InvalidDurableState(err) => Self::Internal(err.int_err()),
            ResourcePersistenceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The reconciliation discards its outcome on a deletion request or a newer
/// generation before converting, so those expectations reach it only as
/// internal errors
impl<R> From<ResourceLifecycleTransitionError<R>> for ReconcileResourceUseCaseError<R>
where
    R: EventSourcedResource,
{
    fn from(err: ResourceLifecycleTransitionError<R>) -> Self {
        use ResourceLifecycleTransitionError as E;

        match err {
            E::Stale(ResourceStaleExpectationError::StatusGenerationChanged(conflict)) => {
                Self::StatusConflict(conflict)
            }
            E::Stale(
                stale @ (ResourceStaleExpectationError::DeletionRequested(_)
                | ResourceStaleExpectationError::GenerationChanged(_)),
            ) => Self::Internal(stale.int_err()),
            E::LoadFailed(err) => Self::LoadFailed(err),
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
            E::Lifecycle(err) => Self::Lifecycle(err),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
