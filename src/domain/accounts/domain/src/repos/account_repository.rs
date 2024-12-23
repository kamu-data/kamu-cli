// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;

use database_common::{EntityPageListing, EntityPageStream, EntityPageStreamer, PaginationOpts};
use internal_error::{InternalError, ResultIntoInternal};
use thiserror::Error;

use crate::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountRepository: Send + Sync {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError>;

    async fn get_account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Account, GetAccountByIdError>;

    async fn get_accounts_by_ids(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<Vec<Account>, GetAccountByIdError>;

    async fn get_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Account, GetAccountByNameError>;

    async fn find_account_id_by_provider_identity_key(
        &self,
        provider_identity_key: &str,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByProviderIdentityKeyError>;

    async fn find_account_id_by_email(
        &self,
        email: &str,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByEmailError>;

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByNameError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait ExpensiveAccountRepository: AccountRepository {
    async fn accounts_count(&self) -> Result<usize, AccountsCountError>;

    async fn get_accounts(&self, pagination: PaginationOpts) -> AccountPageStream;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: tests
#[async_trait::async_trait]
pub trait ExpensiveAccountRepositoryExt: ExpensiveAccountRepository {
    fn all_accounts(&self) -> AccountPageStream;
}

#[async_trait::async_trait]
impl<T> ExpensiveAccountRepositoryExt for T
where
    T: ExpensiveAccountRepository,
    T: ?Sized,
{
    fn all_accounts(&self) -> AccountPageStream {
        EntityPageStreamer::default().into_stream(
            || async { Ok(()) },
            move |_, pagination| async move {
                use futures::TryStreamExt;

                let total_count = self.accounts_count().await.int_err()?;
                let entries = self.get_accounts(pagination).await.try_collect().await?;

                Ok(EntityPageListing {
                    list: entries,
                    total_count,
                })
            },
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccountPageStream<'a> = EntityPageStream<'a, Account>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AccountsCountError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateAccountError {
    #[error(transparent)]
    Duplicate(CreateAccountErrorDuplicate),

    #[error(transparent)]
    Internal(#[from] InternalError),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountByIdError {
    #[error(transparent)]
    NotFound(AccountNotFoundByIdError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Account not found by ID: '{account_id}'")]
pub struct AccountNotFoundByIdError {
    pub account_id: odf::AccountID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccountByNameError {
    #[error(transparent)]
    NotFound(AccountNotFoundByNameError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Account not found by name: '{account_name}'")]
pub struct AccountNotFoundByNameError {
    pub account_name: odf::AccountName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByProviderIdentityKeyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByEmailError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountIdByNameError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
