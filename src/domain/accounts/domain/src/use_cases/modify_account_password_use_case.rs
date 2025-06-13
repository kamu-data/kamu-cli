// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, InternalError};
use thiserror::Error;

use crate::{
    Account,
    IncorrectPasswordError,
    ModifyPasswordHashError,
    Password,
    VerifyPasswordError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ModifyAccountPasswordUseCase: Send + Sync {
    async fn execute(
        &self,
        account: &Account,
        password: Password,
    ) -> Result<(), ModifyAccountPasswordError>;

    async fn execute_with_confirmation(
        &self,
        account: &Account,
        old_password: Password,
        new_password: Password,
    ) -> Result<(), ModifyAccountPasswordWithConfirmationError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ModifyAccountPasswordError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<ModifyPasswordHashError> for ModifyAccountPasswordError {
    fn from(value: ModifyPasswordHashError) -> Self {
        use ModifyPasswordHashError as E;

        match value {
            e @ (E::AccountNotFound(_) | E::Internal(_)) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ModifyAccountPasswordWithConfirmationError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    IncorrectPassword(#[from] IncorrectPasswordError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<VerifyPasswordError> for ModifyAccountPasswordWithConfirmationError {
    fn from(e: VerifyPasswordError) -> Self {
        use VerifyPasswordError as E;

        match e {
            E::IncorrectPassword(e) => Self::IncorrectPassword(e),
            e @ (E::AccountNotFound(_) | E::Internal(_)) => Self::Internal(e.int_err()),
        }
    }
}

impl From<ModifyAccountPasswordError> for ModifyAccountPasswordWithConfirmationError {
    fn from(e: ModifyAccountPasswordError) -> Self {
        use ModifyAccountPasswordError as E;

        match e {
            E::Access(e) => Self::Access(e),
            e @ E::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
