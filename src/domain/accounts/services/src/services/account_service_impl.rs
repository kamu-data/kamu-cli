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

use chrono::Utc;
use crypto_utils::{Argon2Hasher, PasswordHashingMode};
use database_common::{BatchLookup, BatchLookupCreateOptions, PaginationOpts};
use email_utils::Email;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::*;
use odf::metadata::DidPkh;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountServiceImpl {
    account_repo: Arc<dyn AccountRepository>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AccountService)]
impl AccountServiceImpl {
    fn new(
        account_repo: Arc<dyn AccountRepository>,
        password_hash_repository: Arc<dyn PasswordHashRepository>,
        maybe_password_hashing_mode: Option<Arc<PasswordHashingMode>>,
    ) -> Self {
        Self {
            account_repo,
            password_hash_repository,
            // When hashing mode is unspecified, safely assume the default mode.
            // Higher security by default is better than forgetting to configure
            password_hashing_mode: maybe_password_hashing_mode
                .map_or(PasswordHashingMode::Default, |mode| *mode),
        }
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
        account_ids: &[&odf::AccountID],
    ) -> Result<BatchLookup<Account, odf::AccountID, GetAccountByIdError>, InternalError> {
        let accounts = self
            .account_repo
            .get_accounts_by_ids(account_ids)
            .await
            .int_err()?;

        Ok(BatchLookup::from_found_items(
            accounts,
            account_ids,
            BatchLookupCreateOptions {
                found_ids_fn: |accounts| accounts.iter().map(|a| a.id.clone()).collect(),
                not_found_err_fn: |account_id| {
                    GetAccountByIdError::NotFound(AccountNotFoundByIdError {
                        account_id: (*account_id).clone(),
                    })
                },
                _phantom: Default::default(),
            },
        ))
    }

    async fn get_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Account, GetAccountByNameError> {
        self.account_repo.get_account_by_name(account_name).await
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

    async fn create_wallet_account(&self, did_pkh: &DidPkh) -> Result<Account, CreateAccountError> {
        let wallet_address = did_pkh.wallet_address();
        let new_account = Account {
            id: did_pkh.clone().into(),
            account_name: odf::AccountName::new_unchecked(wallet_address),
            email: Email::parse(&format!("{wallet_address}@example.com")).unwrap(),
            display_name: AccountDisplayName::from(wallet_address),
            account_type: AccountType::User,
            avatar_url: None,
            registered_at: Utc::now(),
            provider: AccountProvider::Web3Wallet.to_string(),
            provider_identity_key: wallet_address.to_string(),
        };

        self.account_repo.save_account(&new_account).await?;

        Ok(new_account)
    }

    async fn delete_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), InternalError> {
        use DeleteAccountByNameError as E;

        match self.account_repo.delete_account_by_name(account_name).await {
            Ok(_) | Err(E::NotFound(_)) => Ok(()),
            Err(e @ E::Internal(_)) => Err(e.int_err()),
        }
    }

    async fn save_account_password(
        &self,
        account: &Account,
        password: &Password,
    ) -> Result<(), InternalError> {
        // Save account password
        let password_hash =
            Argon2Hasher::hash_async(password.as_bytes(), self.password_hashing_mode)
                .await
                .int_err()?;

        self.password_hash_repository
            .save_password_hash(&account.id, password_hash)
            .await
            .int_err()
    }

    async fn verify_account_password(
        &self,
        account_name: &odf::AccountName,
        password: &Password,
    ) -> Result<(), VerifyPasswordError> {
        let password_hash = match self
            .password_hash_repository
            .find_password_hash_by_account_name(account_name)
            .await
        {
            Ok(Some(password_hash)) => password_hash,
            Ok(None) => {
                return Err(AccountNotFoundByNameError {
                    account_name: account_name.clone(),
                }
                .into());
            }
            Err(e) => {
                return Err(VerifyPasswordError::Internal(e.int_err()));
            }
        };

        let is_password_correct = Argon2Hasher::verify_async(
            password.as_bytes(),
            password_hash.as_str(),
            self.password_hashing_mode,
        )
        .await
        .int_err()?;

        if !is_password_correct {
            return Err(VerifyPasswordError::IncorrectPassword(
                IncorrectPasswordError,
            ));
        }

        Ok(())
    }

    async fn modify_account_password(
        &self,
        account_id: &odf::AccountID,
        new_password: &Password,
    ) -> Result<(), ModifyAccountPasswordError> {
        let password_hash =
            Argon2Hasher::hash_async(new_password.as_bytes(), self.password_hashing_mode)
                .await
                .int_err()?;

        self.password_hash_repository
            .modify_password_hash(account_id, password_hash)
            .await
            .int_err()?;

        Ok(())
    }

    async fn save_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        self.account_repo.save_account(account).await
    }

    async fn update_account(&self, account: &Account) -> Result<(), UpdateAccountError> {
        self.account_repo.update_account(account).await
    }

    async fn find_account_id_by_provider_identity_key(
        &self,
        provider_identity_key: &str,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByProviderIdentityKeyError> {
        self.account_repo
            .find_account_id_by_provider_identity_key(provider_identity_key)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
