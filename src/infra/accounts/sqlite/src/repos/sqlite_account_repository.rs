// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use opendatafabric::{AccountID, AccountName};

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteAccountRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl SqliteAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
struct AccountRowModel {
    id: AccountID,
    account_name: String,
    email: Option<String>,
    display_name: String,
    account_type: AccountType,
    avatar_url: Option<String>,
    registered_at: DateTime<Utc>,
    is_admin: i64,
    provider: String,
    provider_identity_key: String,
}

#[async_trait::async_trait]
impl AccountRepository for SqliteAccountRepository {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(CreateAccountError::Internal)?;

        let account_id = account.id.to_string();
        let account_name = account.account_name.to_ascii_lowercase();
        let email = account
            .email
            .as_ref()
            .map(|email| email.to_ascii_lowercase());
        let provider = account.provider.to_string();
        let provider_identity_key = account.provider_identity_key.to_string();

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, is_admin, provider, provider_identity_key)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            account_id,
            account_name,
            email,
            account.display_name,
            account.account_type,
            account.avatar_url,
            account.registered_at,
            account.is_admin,
            provider,
            provider_identity_key
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    let sqlite_error_message = e.message();

                    let account_field = if sqlite_error_message.contains("accounts.id") {
                        CreateAccountDuplicateField::Id
                    } else if sqlite_error_message.contains("accounts.account_name") {
                        CreateAccountDuplicateField::Name
                    } else if sqlite_error_message.contains("accounts.email") {
                        CreateAccountDuplicateField::Email
                    } else if sqlite_error_message.contains("accounts.provider_identity_key") {
                        CreateAccountDuplicateField::ProviderIdentityKey
                    } else {
                        tracing::error!("Unexpected SQLite error message: {}", sqlite_error_message);
                        CreateAccountDuplicateField::Id
                    };

                    CreateAccountError::Duplicate(CreateAccountErrorDuplicate {
                        account_field
                    })
                } else {
                    CreateAccountError::Internal(e.int_err())
                }
            }
            _ => CreateAccountError::Internal(e.int_err())
        })?;

        Ok(())
    }

    async fn get_account_by_id(
        &self,
        account_id: &AccountID,
    ) -> Result<Account, GetAccountByIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccountByIdError::Internal)?;

        let account_id_str = account_id.to_string();

        let maybe_account_row = sqlx::query_as!(
            AccountRowModel,
            r#"
            SELECT
                id as "id: AccountID",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at as "registered_at: _",
                is_admin,
                provider,
                provider_identity_key
            FROM accounts
            WHERE id = $1
            "#,
            account_id_str
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetAccountByIdError::Internal)?;

        if let Some(account_row) = maybe_account_row {
            Ok(Account {
                id: account_row.id,
                account_name: AccountName::new_unchecked(&account_row.account_name),
                email: account_row.email,
                display_name: account_row.display_name,
                account_type: account_row.account_type,
                avatar_url: account_row.avatar_url,
                registered_at: account_row.registered_at,
                is_admin: account_row.is_admin != 0,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
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
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccountByNameError::Internal)?;

        let account_name_str = account_name.to_string();

        let maybe_account_row = sqlx::query_as!(
            AccountRowModel,
            r#"
            SELECT
                id as "id: AccountID",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at as "registered_at: _",
                is_admin,
                provider,
                provider_identity_key
            FROM accounts
            WHERE lower(account_name) = lower($1)
            "#,
            account_name_str
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(GetAccountByNameError::Internal)?;

        if let Some(account_row) = maybe_account_row {
            Ok(Account {
                id: account_row.id,
                account_name: AccountName::new_unchecked(&account_row.account_name),
                email: account_row.email,
                display_name: account_row.display_name,
                account_type: account_row.account_type,
                avatar_url: account_row.avatar_url,
                registered_at: account_row.registered_at,
                is_admin: account_row.is_admin != 0,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
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
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByProviderIdentityKeyError::Internal)?;

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE provider_identity_key = $1
            "#,
            provider_identity_key
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindAccountIdByProviderIdentityKeyError::Internal)?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByEmailError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByEmailError::Internal)?;

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(email) = lower($1)
            "#,
            email
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindAccountIdByEmailError::Internal)?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, FindAccountIdByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByNameError::Internal)?;

        let account_name_str = account_name.to_string();
        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(account_name) = lower($1)
            "#,
            account_name_str
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindAccountIdByNameError::Internal)?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for SqliteAccountRepository {
    async fn save_password_hash(
        &self,
        account_name: &AccountName,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(SavePasswordHashError::Internal)?;

        // TODO: duplicates are prevented with unique indices, but handle error

        let account_name = account_name.to_string();
        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_name, password_hash)
                VALUES ($1, $2)
            "#,
            account_name,
            password_hash
        )
        .execute(connection_mut)
        .await
        .int_err()
        .map_err(SavePasswordHashError::Internal)?;

        Ok(())
    }

    async fn find_password_hash_by_account_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<String>, FindPasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindPasswordHashError::Internal)?;

        let account_name = account_name.to_string();
        let maybe_password_row = sqlx::query!(
            r#"
            SELECT password_hash
              FROM accounts_passwords
              WHERE lower(account_name) = lower($1)
            "#,
            account_name,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindPasswordHashError::Internal)?;

        Ok(maybe_password_row.map(|password_row| password_row.password_hash))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
