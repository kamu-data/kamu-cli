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

use crypto_utils::{Argon2Hasher, Hasher, PasswordHashingMode};
use database_common::PaginationOpts;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::*;
use secrecy::{ExposeSecret, SecretString};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountServiceImpl {
    did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
    account_repo: Arc<dyn AccountRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    did_secret_encryption_key: Option<SecretString>,
    password_hash_repository: Arc<dyn PasswordHashRepository>,
    password_hashing_mode: PasswordHashingMode,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AccountService)]
impl AccountServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    fn new(
        did_secret_key_repo: Arc<dyn DidSecretKeyRepository>,
        account_repo: Arc<dyn AccountRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        did_secret_encryption_config: Arc<DidSecretEncryptionConfig>,
        password_hash_repository: Arc<dyn PasswordHashRepository>,
        password_hashing_mode: Option<Arc<PasswordHashingMode>>,
    ) -> Self {
        Self {
            did_secret_key_repo,
            account_repo,
            time_source,
            did_secret_encryption_key: did_secret_encryption_config
                .encryption_key
                .as_ref()
                .map(|encryption_key| SecretString::from(encryption_key.clone())),
            password_hash_repository,
            password_hashing_mode: password_hashing_mode
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

    async fn create_account(
        &self,
        account_name: &odf::AccountName,
        email: email_utils::Email,
        password: Password,
    ) -> Result<Account, CreateAccountError> {
        let (account_key, account_id) = odf::AccountID::new_generated_ed25519();
        let account = Account {
            id: account_id,
            account_name: account_name.clone(),
            email,
            display_name: account_name.to_string(),
            account_type: AccountType::User,
            avatar_url: None,
            registered_at: self.time_source.now(),
            provider: String::from(PROVIDER_PASSWORD),
            provider_identity_key: String::from(account_name.as_str()),
        };

        self.account_repo.save_account(&account).await?;

        if let Some(did_secret_encryption_key) = &self.did_secret_encryption_key {
            let account_id = account.id.as_did_str().to_stack_string();
            let did_secret_key = DidSecretKey::try_new(
                &account_key.into(),
                did_secret_encryption_key.expose_secret(),
            )
            .int_err()?;

            self.did_secret_key_repo
                .save_did_secret_key(
                    &DidEntity::new_account(account_id.as_str()),
                    &did_secret_key,
                )
                .await
                .int_err()?;
        }

        let hashing_mode = self.password_hashing_mode;
        let password_hash = tokio::task::spawn_blocking(move || {
            let argon2_hasher = Argon2Hasher::new(hashing_mode);
            argon2_hasher.hash(password.as_bytes())
        })
        .await
        .int_err()?;

        self.password_hash_repository
            .save_password_hash(&account.id, &account.account_name, password_hash)
            .await
            .int_err()?;

        Ok(account)
    }

    async fn modify_password(
        &self,
        account_name: &odf::AccountName,
        password: Password,
    ) -> Result<(), ModifyPasswordError> {
        let hashing_mode = self.password_hashing_mode;
        let password_hash = tokio::task::spawn_blocking(move || {
            let argon2_hasher = Argon2Hasher::new(hashing_mode);
            argon2_hasher.hash(password.as_bytes())
        })
        .await
        .int_err()?;

        self.password_hash_repository
            .modify_password_hash(account_name, password_hash)
            .await
            .int_err()?;

        Ok(())
    }

    async fn delete_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), InternalError> {
        use DeleteAccountError as E;

        match self.account_repo.delete_account_by_name(account_name).await {
            Ok(_) | Err(E::NotFound(_)) => Ok(()),
            Err(e @ E::Internal(_)) => Err(e.int_err()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
