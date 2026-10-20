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

use crate::{EventSourcedResource, ResourceID, ResourceLoadError, ResourcePersistenceError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs the cleanup of the main controller for resources whose deletion was
/// requested, each in a transaction of its own, then completes their deletion.
/// A failed cleanup, a database error included, is recorded in the status and
/// leaves that deletion in progress; it is not an error of the use case.
/// Resources that do not wait for a cleanup are skipped.
#[cfg_attr(feature = "testing", mockall::automock)]
#[async_trait::async_trait]
pub trait CompleteResourceDeletionUseCase<R: EventSourcedResource>: Send + Sync {
    async fn execute(
        &self,
        ids: &[ResourceID],
    ) -> Result<(), CompleteResourceDeletionUseCaseError<R>>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CompleteResourceDeletionUseCaseError<R: EventSourcedResource> {
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

impl<R> From<ResourcePersistenceError> for CompleteResourceDeletionUseCaseError<R>
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
