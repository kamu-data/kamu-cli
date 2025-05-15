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

use crate::{AccountNotFoundByNameError, ModifyPasswordHashError, Password};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ModifyPasswordUseCase: Send + Sync {
    async fn execute(
        &self,
        account_name: &odf::AccountName,
        password: Password,
    ) -> Result<(), ModifyPasswordError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum ModifyPasswordError {
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    AccountNotFound(#[from] AccountNotFoundByNameError),
}

impl From<ModifyPasswordHashError> for ModifyPasswordError {
    fn from(value: ModifyPasswordHashError) -> Self {
        match value {
            ModifyPasswordHashError::AccountNotFound(err) => Self::AccountNotFound(err),
            ModifyPasswordHashError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
