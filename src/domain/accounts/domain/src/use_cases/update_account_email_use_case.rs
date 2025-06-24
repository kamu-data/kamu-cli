// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use email_utils::Email;
use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{Account, AccountErrorDuplicate, UpdateAccountError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateAccountEmailUseCase: Send + Sync {
    async fn execute(
        &self,
        account: &Account,
        new_email: Email,
    ) -> Result<(), UpdateAccountEmailError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum UpdateAccountEmailError {
    #[error(transparent)]
    Duplicate(AccountErrorDuplicate),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<UpdateAccountError> for UpdateAccountEmailError {
    fn from(err: UpdateAccountError) -> Self {
        match err {
            UpdateAccountError::Duplicate(e) => UpdateAccountEmailError::Duplicate(e),
            UpdateAccountError::NotFound(e) => UpdateAccountEmailError::Internal(e.int_err()),
            UpdateAccountError::Internal(e) => UpdateAccountEmailError::Internal(e),
        }
    }
}
