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

use crate::{Account, AccountNotFoundByNameError, DeleteAccountError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DeleteAccountUseCase: Send + Sync {
    async fn execute(&self, account: &Account) -> Result<(), DeleteAccountByNameError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum DeleteAccountByNameError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    NotFound(AccountNotFoundByNameError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<DeleteAccountError> for DeleteAccountByNameError {
    fn from(e: DeleteAccountError) -> Self {
        use internal_error::ErrorIntoInternal;

        match e {
            DeleteAccountError::NotFound(e) => Self::NotFound(e),
            DeleteAccountError::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
