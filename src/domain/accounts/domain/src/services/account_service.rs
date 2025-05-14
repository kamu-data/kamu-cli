// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use database_common::PaginationOpts;
use internal_error::InternalError;
use thiserror::Error;

use crate::{
    Account,
    AccountNotFoundByNameError,
    AccountPageStream,
    CreateAccountError,
    DeleteAccountError,
    GetAccountByIdError,
    ModifyPasswordHashError,
    Password,
    SearchAccountsByNamePatternFilters,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountService: Sync + Send {
    async fn get_account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Account, GetAccountByIdError>;

    async fn get_accounts_by_ids(
        &self,
        account_ids: &[odf::AccountID],
    ) -> Result<Vec<Account>, InternalError>;

    async fn get_account_map(
        &self,
        account_ids: &[odf::AccountID],
    ) -> Result<HashMap<odf::AccountID, Account>, GetAccountMapError>;

    async fn account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<Account>, InternalError>;

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, InternalError>;

    async fn find_account_name_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<odf::AccountName>, InternalError>;

    fn search_accounts_by_name_pattern<'a>(
        &'a self,
        name_pattern: &'a str,
        filters: SearchAccountsByNamePatternFilters,
        pagination: PaginationOpts,
    ) -> AccountPageStream<'a>;

    async fn create_account(
        &self,
        account_name: &odf::AccountName,
        email: email_utils::Email,
        password: Password,
    ) -> Result<Account, CreateAccountError>;

    async fn modify_password(
        &self,
        account_name: &odf::AccountName,
        password: Password,
    ) -> Result<(), ModifyPasswordError>;

    async fn delete_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), DeleteAccountError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountServiceExt {
    async fn account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<Account>, InternalError>;
}

#[async_trait::async_trait]
impl<T: AccountService + ?Sized> AccountServiceExt for T {
    async fn account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<Account>, InternalError> {
        match self.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account)),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Error
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetAccountMapError {
    #[error(transparent)]
    Internal(#[from] InternalError),
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
