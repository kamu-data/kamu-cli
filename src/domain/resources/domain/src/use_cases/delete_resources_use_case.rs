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

use crate::{DeclarativeResource, FindOwnedResourceError, ResourceUID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteResourcesUseCase<R: DeclarativeResource>: Send + Sync {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        uids: Vec<ResourceUID>,
    ) -> Result<(), DeleteResourcesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteResourcesError {
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

impl From<FindOwnedResourceError> for DeleteResourcesError {
    fn from(err: FindOwnedResourceError) -> Self {
        match err {
            FindOwnedResourceError::Access(err) => Self::Access(err),
            FindOwnedResourceError::TypeMismatch(err) => Self::Internal(err.int_err()),
            FindOwnedResourceError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
