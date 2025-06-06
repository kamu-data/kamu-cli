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

use crate::{Account, AccountNotFoundByNameError, ModifyPasswordHashError, Password};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ModifyAccountPasswordUseCase: Send + Sync {
    async fn execute(
        &self,
        account: &Account,
        password: Password,
    ) -> Result<(), ModifyAccountPasswordError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ModifyAccountPasswordError {
    #[error(transparent)]
    Internal(#[from] InternalError),

    #[error(transparent)]
    AccountNotFound(#[from] AccountNotFoundByNameError),
}

impl From<ModifyPasswordHashError> for ModifyAccountPasswordError {
    fn from(value: ModifyPasswordHashError) -> Self {
        match value {
            ModifyPasswordHashError::AccountNotFound(err) => Self::AccountNotFound(err),
            e @ ModifyPasswordHashError::Internal(_) => Self::Internal(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
