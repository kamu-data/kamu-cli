// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    Account,
    AccountPageStream,
    AccountRepository,
    AccountService,
    FindAccountIdByNameError,
    GetAccountByIdError,
    GetAccountByNameError,
    GetAccountMapError,
    SearchAccountsByNamePatternFilters,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountServiceImpl {
    account_repo: Arc<dyn AccountRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccountService)]
impl AccountServiceImpl {
    pub fn new(account_repo: Arc<dyn AccountRepository>) -> Self {
        Self { account_repo }
    }
}

#[async_trait::async_trait]
impl AccountService for AccountServiceImpl {
    async fn get_account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Account, GetAccountByIdError> {
        self.account_repo.get_account_by_id(account_id).await
    }

    async fn get_accounts_by_ids(
        &self,
        account_ids: &[odf::AccountID],
    ) -> Result<Vec<Account>, InternalError> {
        self.account_repo
            .get_accounts_by_ids(account_ids)
            .await
            .int_err()
    }

    async fn get_account_map(
        &self,
        account_ids: &[odf::AccountID],
    ) -> Result<HashMap<odf::AccountID, Account>, GetAccountMapError> {
        let account_map = match self.account_repo.get_accounts_by_ids(account_ids).await {
            Ok(accounts) => {
                let map = accounts
                    .into_iter()
                    .fold(HashMap::new(), |mut acc, account| {
                        acc.insert(account.id.clone(), account);
                        acc
                    });
                Ok(map)
            }
            Err(err) => match err {
                GetAccountByIdError::NotFound(_) => Ok(HashMap::new()),
                e => Err(e),
            },
        }
        .int_err()?;

        Ok(account_map)
    }

    async fn account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<Account>, InternalError> {
        match self.account_repo.get_account_by_name(account_name).await {
            Ok(account) => Ok(Some(account.clone())),
            Err(GetAccountByNameError::NotFound(_)) => Ok(None),
            Err(GetAccountByNameError::Internal(e)) => Err(e),
        }
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, InternalError> {
        match self
            .account_repo
            .find_account_id_by_name(account_name)
            .await
        {
            Ok(maybe_account_id) => Ok(maybe_account_id),
            Err(FindAccountIdByNameError::Internal(e)) => Err(e),
        }
    }

    async fn find_account_name_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Option<odf::AccountName>, InternalError> {
        match self.account_repo.get_account_by_id(account_id).await {
            Ok(account) => Ok(Some(account.account_name.clone())),
            Err(GetAccountByIdError::NotFound(_)) => Ok(None),
            Err(GetAccountByIdError::Internal(e)) => Err(e),
        }
    }

    fn search_accounts_by_name_pattern<'a>(
        &'a self,
        name_pattern: &'a str,
        filters: SearchAccountsByNamePatternFilters,
        pagination: PaginationOpts,
    ) -> AccountPageStream<'a> {
        self.account_repo
            .search_accounts_by_name_pattern(name_pattern, filters, pagination)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
