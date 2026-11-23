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
use thiserror::Error;

use crate::{
    DeclarativeResource,
    EventSourcedResource,
    FindOwnedResourceError,
    ResourceID,
    ResourceLifecycleTransitionError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait RequestResourcesDeletionUseCase<R: DeclarativeResource>: Send + Sync {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        ids: Vec<ResourceID>,
    ) -> Result<(), RequestResourcesDeletionError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum RequestResourcesDeletionError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<FindOwnedResourceError> for RequestResourcesDeletionError {
    fn from(err: FindOwnedResourceError) -> Self {
        match err {
            FindOwnedResourceError::Access(err) => Self::Access(err),
            FindOwnedResourceError::TypeMismatch(err) => Self::Internal(err.int_err()),
            FindOwnedResourceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<R> From<ResourceLifecycleTransitionError<R>> for RequestResourcesDeletionError
where
    R: EventSourcedResource,
    R::LifecycleError: std::fmt::Display,
{
    fn from(err: ResourceLifecycleTransitionError<R>) -> Self {
        use ResourceLifecycleTransitionError as E;

        match err {
            E::ConcurrentModification(err) => Self::ConcurrentModification(err),
            E::LoadFailed(err) => Self::Internal(
                format!("{err}")
                    .int_err()
                    .with_context("Failed to load resources"),
            ),
            E::Lifecycle(err) => Self::Internal(
                format!("{err}")
                    .int_err()
                    .with_context("Failed to request deletion of resources"),
            ),
            E::Stale(err) => Self::Internal(err.int_err()),
            E::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
