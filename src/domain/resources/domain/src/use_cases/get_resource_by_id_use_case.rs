// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{
    DeclarativeResource,
    ResourceID,
    ResourceIDNotFoundError,
    ResourceTypeMismatchError,
    TypedResourceQueryError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GetResourceByIdUseCase<R: DeclarativeResource>: Send + Sync {
    async fn execute(
        &self,
        account_id: odf::AccountID,
        id: &ResourceID,
    ) -> Result<R::ResourceState, GetResourceByIdError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum GetResourceByIdError {
    #[error(transparent)]
    NotFound(#[from] ResourceIDNotFoundError),

    #[error(transparent)]
    TypeMismatch(#[from] ResourceTypeMismatchError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<TypedResourceQueryError> for GetResourceByIdError {
    fn from(err: TypedResourceQueryError) -> Self {
        match err {
            TypedResourceQueryError::NotFound(err) => Self::NotFound(err),
            TypedResourceQueryError::TypeMismatch(err) => Self::TypeMismatch(err),
            TypedResourceQueryError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
