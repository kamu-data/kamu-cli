// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use internal_error::InternalError;
use opendatafabric::{AccountID, AccountName};
use thiserror::Error;

use crate::Account;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountRepository: Send + Sync {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError>;

    async fn get_account_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Account, GetAccountByIdError>;

    async fn get_accounts_by_ids(
        &self,
        account_ids: Vec<AccountID>,
    ) -> Result<Vec<Account>, GetAccountByIdError>;

    async fn get_account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Account, GetAccountByNameError>;

    async fn find_account_id_by_provider_identity_key(
        &self,
        provider_identity_key: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByProviderIdentityKeyError>;

    async fn find_account_id_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByEmailError>;

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, FindAccountIdByNameError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateAccountError {
    #[error(transparent)]
    Internal(InternalError),

    #[error(transparent)]
    Duplicate(CreateAccountErrorDuplicate),
}

#[derive(Error, Debug)]
#[error("Account not created, duplicate {account_field}")]
pub struct CreateAccountErrorDuplicate {
    pub account_field: CreateAccountDuplicateField,
}

#[derive(Debug, PartialEq, Eq)]
pub enum CreateAccountDuplicateField {
    Id,
    Name,
    Email,
    ProviderIdentityKey,
}

impl Display for CreateAccountDuplicateField {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                Self::Id => "id",
                Self::Name => "name",
                Self::Email => "email",
                Self::ProviderIdentityKey => "provider identity",
            },
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountByIdError {
    #[error(transparent)]
    NotFound(AccountNotFoundByIdError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Account not found by ID: '{account_id}'")]
pub struct AccountNotFoundByIdError {
    pub account_id: AccountID,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountByNameError {
    #[error(transparent)]
    NotFound(AccountNotFoundByNameError),

    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Error, Debug)]
#[error("Account not found by name: '{account_name}'")]
pub struct AccountNotFoundByNameError {
    pub account_name: AccountName,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByProviderIdentityKeyError {
    #[error(transparent)]
    Internal(InternalError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByEmailError {
    #[error(transparent)]
    Internal(InternalError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByNameError {
    #[error(transparent)]
    Internal(InternalError),
}

///////////////////////////////////////////////////////////////////////////////
