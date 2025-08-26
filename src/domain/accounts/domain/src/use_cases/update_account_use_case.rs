// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::{Account, AccountErrorDuplicate, AccountNotFoundByIdError, UpdateAccountError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateAccountUseCase: Send + Sync {
    async fn execute(&self, account: &Account) -> Result<(), UpdateAccountUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum UpdateAccountUseCaseError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    NotFound(AccountNotFoundByIdError),

    #[error(transparent)]
    Duplicate(AccountErrorDuplicate),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<UpdateAccountError> for UpdateAccountUseCaseError {
    fn from(err: UpdateAccountError) -> Self {
        match err {
            UpdateAccountError::NotFound(e) => UpdateAccountUseCaseError::NotFound(e),
            UpdateAccountError::Duplicate(e) => UpdateAccountUseCaseError::Duplicate(e),
            UpdateAccountError::Internal(e) => UpdateAccountUseCaseError::Internal(e),
        }
    }
}
