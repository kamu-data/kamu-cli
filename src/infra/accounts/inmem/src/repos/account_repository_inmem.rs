// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dill::*;
use opendatafabric::{AccountID, AccountName};

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountRepositoryInMemory {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    accounts_by_id: HashMap<AccountID, Account>,
    accounts_by_name: HashMap<AccountName, Account>,
    account_id_by_provider_identity_key: HashMap<String, AccountID>,
    password_hash_by_account_name: HashMap<AccountName, String>,
}

impl State {
    fn new() -> Self {
        Self {
            accounts_by_id: HashMap::new(),
            accounts_by_name: HashMap::new(),
            account_id_by_provider_identity_key: HashMap::new(),
            password_hash_by_account_name: HashMap::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn PasswordHashRepository)]
#[scope(Singleton)]
impl AccountRepositoryInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountRepository for AccountRepositoryInMemory {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut guard = self.state.lock().unwrap();
        if guard.accounts_by_id.get(&account.id).is_some() {
            return Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate {
                account_field: CreateAccountDuplicateField::Id,
            }));
        }
        if guard.accounts_by_name.get(&account.account_name).is_some() {
            return Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate {
                account_field: CreateAccountDuplicateField::Name,
            }));
        }
        if guard
            .account_id_by_provider_identity_key
            .get(&account.provider_identity_key)
            .is_some()
        {
            return Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate {
                account_field: CreateAccountDuplicateField::ProviderIdentityKey,
            }));
        }
        if let Some(email) = &account.email {
            for account in guard.accounts_by_id.values() {
                if let Some(account_email) = &account.email
                    && account_email.eq_ignore_ascii_case(email)
                {
                    return Err(CreateAccountError::Duplicate(CreateAccountErrorDuplicate {
                        account_field: CreateAccountDuplicateField::Email,
                    }));
                }
            }
        }

        guard
            .accounts_by_id
            .insert(account.id.clone(), account.clone());
        guard
            .accounts_by_name
            .insert(account.account_name.clone(), account.clone());
        guard
            .account_id_by_provider_identity_key
            .insert(account.provider_identity_key.clone(), account.id.clone());

        Ok(())
    }

    async fn get_account_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Account, GetAccountByIdError> {
        let guard = self.state.lock().unwrap();
        if let Some(account_data) = guard.accounts_by_id.get(account_id) {
            Ok(account_data.clone())
        } else {
            Err(GetAccountByIdError::NotFound(AccountNotFoundByIdError {
                account_id: account_id.clone(),
            }))
        }
    }
    async fn get_account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Account, GetAccountByNameError> {
        let guard = self.state.lock().unwrap();
        if let Some(account_data) = guard.accounts_by_name.get(account_name) {
            Ok(account_data.clone())
        } else {
            Err(GetAccountByNameError::NotFound(
                AccountNotFoundByNameError {
                    account_name: account_name.clone(),
                },
            ))
        }
    }

    async fn find_account_id_by_provider_identity_key(
        &self,
        provider_identity_key: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByProviderIdentityKeyError> {
        let guard = self.state.lock().unwrap();
        let maybe_account_id = guard
            .account_id_by_provider_identity_key
            .get(provider_identity_key);
        Ok(maybe_account_id.cloned())
    }

    async fn find_account_id_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByEmailError> {
        let guard = self.state.lock().unwrap();
        for account in guard.accounts_by_id.values() {
            if let Some(account_email) = &account.email
                && account_email.eq_ignore_ascii_case(email)
            {
                return Ok(Some(account.id.clone()));
            }
        }
        Ok(None)
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, FindAccountIdByNameError> {
        let guard = self.state.lock().unwrap();
        let maybe_account = guard.accounts_by_name.get(account_name);
        Ok(maybe_account.map(|a| a.id.clone()))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for AccountRepositoryInMemory {
    async fn save_password_hash(
        &self,
        account_name: &AccountName,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut guard = self.state.lock().unwrap();
        guard
            .password_hash_by_account_name
            .insert(account_name.clone(), password_hash);
        Ok(())
    }

    async fn find_password_hash_by_account_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<String>, FindPasswordHashError> {
        let guard = self.state.lock().unwrap();
        let maybe_hash_as_string = guard
            .password_hash_by_account_name
            .get(account_name)
            .cloned();
        Ok(maybe_hash_as_string)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
