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

use database_common::PaginationOpts;
use dill::*;
use email_utils::Email;
use odf::AccountName;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryAccountRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    accounts_by_id: HashMap<odf::AccountID, Account>,
    accounts_by_name: HashMap<odf::AccountName, Account>,
    account_id_by_provider_identity_key: HashMap<String, odf::AccountID>,
    password_hash_by_account_name: HashMap<odf::AccountName, String>,
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

    fn check_unique_name(&self, account_name: &AccountName) -> Result<(), AccountErrorDuplicate> {
        if self.accounts_by_name.contains_key(account_name) {
            return Err(AccountErrorDuplicate {
                account_field: AccountDuplicateField::Name,
            });
        }

        Ok(())
    }

    fn check_unique_provider_identity_key(
        &self,
        provider_identity_key: &String,
    ) -> Result<(), AccountErrorDuplicate> {
        if self
            .account_id_by_provider_identity_key
            .contains_key(provider_identity_key)
        {
            return Err(AccountErrorDuplicate {
                account_field: AccountDuplicateField::ProviderIdentityKey,
            });
        }

        Ok(())
    }

    fn check_unique_email(&self, email: &Email) -> Result<(), AccountErrorDuplicate> {
        for other_account in self.accounts_by_id.values() {
            if other_account
                .email
                .as_ref()
                .eq_ignore_ascii_case(email.as_ref())
            {
                return Err(AccountErrorDuplicate {
                    account_field: AccountDuplicateField::Email,
                });
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn ExpensiveAccountRepository)]
#[interface(dyn PasswordHashRepository)]
#[scope(Singleton)]
impl InMemoryAccountRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountRepository for InMemoryAccountRepository {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut guard = self.state.lock().unwrap();
        if guard.accounts_by_id.contains_key(&account.id) {
            return Err(CreateAccountError::Duplicate(AccountErrorDuplicate {
                account_field: AccountDuplicateField::Id,
            }));
        }

        guard
            .check_unique_name(&account.account_name)
            .map_err(CreateAccountError::Duplicate)?;
        guard
            .check_unique_provider_identity_key(&account.provider_identity_key)
            .map_err(CreateAccountError::Duplicate)?;
        guard
            .check_unique_email(&account.email)
            .map_err(CreateAccountError::Duplicate)?;

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

    async fn update_account(&self, updated_account: Account) -> Result<(), UpdateAccountError> {
        let mut guard = self.state.lock().unwrap();
        let Some(account) = guard.accounts_by_id.get(&updated_account.id).cloned() else {
            return Err(UpdateAccountError::NotFound(AccountNotFoundByIdError {
                account_id: updated_account.id.clone(),
            }));
        };

        if account == updated_account {
            return Ok(());
        }

        if updated_account.account_name != account.account_name {
            guard
                .check_unique_name(&updated_account.account_name)
                .map_err(UpdateAccountError::Duplicate)?;
        }
        if updated_account.provider_identity_key != account.provider_identity_key {
            guard
                .check_unique_provider_identity_key(&updated_account.provider_identity_key)
                .map_err(UpdateAccountError::Duplicate)?;
        }
        if updated_account.email != account.email {
            guard
                .check_unique_email(&updated_account.email)
                .map_err(UpdateAccountError::Duplicate)?;
        }

        guard
            .accounts_by_id
            .insert(updated_account.id.clone(), updated_account.clone());

        if updated_account.account_name != account.account_name {
            guard.accounts_by_name.remove(&account.account_name);
        }
        guard.accounts_by_name.insert(
            updated_account.account_name.clone(),
            updated_account.clone(),
        );

        if updated_account.provider_identity_key != account.provider_identity_key {
            guard
                .account_id_by_provider_identity_key
                .remove(&account.provider_identity_key);
        }
        guard.account_id_by_provider_identity_key.insert(
            updated_account.provider_identity_key.clone(),
            updated_account.id.clone(),
        );

        Ok(())
    }

    async fn update_account_email(
        &self,
        account_id: &odf::AccountID,
        new_email: Email,
    ) -> Result<(), UpdateAccountError> {
        let account_name = {
            let guard = self.state.lock().unwrap();
            let Some(account) = guard.accounts_by_id.get(account_id) else {
                return Err(UpdateAccountError::NotFound(AccountNotFoundByIdError {
                    account_id: account_id.clone(),
                }));
            };

            if new_email != account.email {
                guard
                    .check_unique_email(&new_email)
                    .map_err(UpdateAccountError::Duplicate)?;
            }

            account.account_name.clone()
        };

        let mut guard = self.state.lock().unwrap();

        guard
            .accounts_by_id
            .get_mut(account_id)
            .expect("must exist")
            .email = new_email.clone();

        guard
            .accounts_by_name
            .get_mut(&account_name)
            .expect("must exist")
            .email = new_email;

        Ok(())
    }

    async fn get_account_by_id(
        &self,
        account_id: &odf::AccountID,
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

    async fn get_accounts_by_ids(
        &self,
        account_ids: Vec<odf::AccountID>,
    ) -> Result<Vec<Account>, GetAccountByIdError> {
        let guard = self.state.lock().unwrap();

        let accounts: Vec<Account> = account_ids
            .into_iter()
            .filter_map(|account_id| guard.accounts_by_id.get(&account_id).cloned())
            .collect();

        Ok(accounts)
    }

    async fn get_account_by_name(
        &self,
        account_name: &odf::AccountName,
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
    ) -> Result<Option<odf::AccountID>, FindAccountIdByProviderIdentityKeyError> {
        let guard = self.state.lock().unwrap();
        let maybe_account_id = guard
            .account_id_by_provider_identity_key
            .get(provider_identity_key);
        Ok(maybe_account_id.cloned())
    }

    async fn find_account_id_by_email(
        &self,
        email: &Email,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByEmailError> {
        let guard = self.state.lock().unwrap();
        for account in guard.accounts_by_id.values() {
            if account.email.as_ref().eq_ignore_ascii_case(email.as_ref()) {
                return Ok(Some(account.id.clone()));
            }
        }
        Ok(None)
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByNameError> {
        let guard = self.state.lock().unwrap();
        let maybe_account = guard.accounts_by_name.get(account_name);
        Ok(maybe_account.map(|a| a.id.clone()))
    }

    async fn search_accounts_by_name_pattern(
        &self,
        _name_pattern: &str,
    ) -> Result<Vec<Account>, SearchAccountsByNamePatternError> {
        todo!("TODO: Private Datasets: implementation")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ExpensiveAccountRepository for InMemoryAccountRepository {
    async fn accounts_count(&self) -> Result<usize, AccountsCountError> {
        let readable_state = self.state.lock().unwrap();

        let accounts_count = readable_state.accounts_by_id.len();

        Ok(accounts_count)
    }

    async fn get_accounts(&self, pagination: PaginationOpts) -> AccountPageStream {
        let dataset_entries_page = {
            let readable_state = self.state.lock().unwrap();

            readable_state
                .accounts_by_id
                .values()
                .skip(pagination.offset)
                .take(pagination.limit)
                .cloned()
                .map(Ok)
                .collect::<Vec<_>>()
        };

        Box::pin(futures::stream::iter(dataset_entries_page))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for InMemoryAccountRepository {
    async fn save_password_hash(
        &self,
        account_name: &odf::AccountName,
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
        account_name: &odf::AccountName,
    ) -> Result<Option<String>, FindPasswordHashError> {
        let guard = self.state.lock().unwrap();
        let maybe_hash_as_string = guard
            .password_hash_by_account_name
            .get(account_name)
            .cloned();
        Ok(maybe_hash_as_string)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
