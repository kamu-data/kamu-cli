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

use super::AccountNotFoundByIdError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait PasswordHashRepository: Send + Sync {
    async fn save_password_hash(
        &self,
        account_id: &odf::AccountID,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError>;

    async fn modify_password_hash(
        &self,
        account_id: &odf::AccountID,
        password_hash: String,
    ) -> Result<(), ModifyPasswordHashError>;

    async fn find_password_hash_by_account_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<String>, FindPasswordHashError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SavePasswordHashError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ModifyPasswordHashError {
    #[error(transparent)]
    Internal(#[from] InternalError),
    #[error(transparent)]
    AccountNotFound(#[from] AccountNotFoundByIdError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindPasswordHashError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
