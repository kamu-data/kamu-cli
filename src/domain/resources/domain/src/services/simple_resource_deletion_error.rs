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
    ResourceLifecycleTransitionError,
    ResourceLoadError,
    ResourcePersistenceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why completing deletions failed. A failed cleanup is an outcome, recorded
/// in the status, not this error
#[derive(thiserror::Error, Debug)]
pub enum SimpleResourceDeletionError<R: EventSourcedResource> {
    #[error(transparent)]
    LoadFailed(#[from] ResourceLoadError<R::ResourceState>),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    Lifecycle(R::LifecycleError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ResourcePersistenceError> for SimpleResourceDeletionError<R>
where
    R: EventSourcedResource,
{
    fn from(err: ResourcePersistenceError) -> Self {
        match err {
            ResourcePersistenceError::Duplicate(err) => Self::Internal(
                err.int_err()
                    .with_context("Unexpected duplicate resource state while completing deletion"),
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

/// Recording cleanup outcomes skips resources whose expectations no longer
/// hold, so a stale expectation reaches it only as an internal error
impl<R> From<ResourceLifecycleTransitionError<R>> for SimpleResourceDeletionError<R>
where
    R: EventSourcedResource,
{
    fn from(err: ResourceLifecycleTransitionError<R>) -> Self {
        use ResourceLifecycleTransitionError as E;

        match err {
            E::Stale(stale) => Self::Internal(stale.int_err()),
            E::LoadFailed(err) => Self::LoadFailed(err),
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
            E::Lifecycle(err) => Self::Lifecycle(err),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
