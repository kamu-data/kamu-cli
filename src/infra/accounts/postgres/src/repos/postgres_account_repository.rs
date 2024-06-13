// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use opendatafabric::{AccountID, AccountName};

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl PostgresAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(CreateAccountError::Internal)?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, is_admin, provider, provider_identity_key)
                VALUES ($1, $2, $3, $4, ($5::text)::account_type, $6, $7, $8, $9, $10)
            "#,
            account.id.to_string(),
            account.account_name.to_ascii_lowercase(),
            account.email.as_ref().map(|email| email.to_ascii_lowercase()),
            account.display_name,
            account.account_type as AccountType,
            account.avatar_url,
            account.registered_at,
            account.is_admin,
            account.provider.to_string(),
            account.provider_identity_key.to_string(),
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    let account_field = match e.constraint() {
                        Some("accounts_pkey") => CreateAccountDuplicateField::Id,
                        Some("idx_accounts_email") => CreateAccountDuplicateField::Email,
                        Some("idx_accounts_name") => CreateAccountDuplicateField::Name,
                        Some("idx_provider_identity_key") => CreateAccountDuplicateField::ProviderIdentityKey,
                        _ => {
                            tracing::error!("Unexpected Postgres error message: {}", e.message());
                            CreateAccountDuplicateField::Id
                        }
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

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT
                id as "id: AccountID",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at,
                is_admin,
                provider,
                provider_identity_key
            FROM accounts
            WHERE id = $1
            "#,
            account_id.to_string()
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
                is_admin: account_row.is_admin,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
        } else {
            Err(GetAccountByIdError::NotFound(AccountNotFoundByIdError {
                account_id: account_id.clone(),
            }))
        }
    }

    async fn get_accounts_by_ids(
        &self,
        account_ids: Vec<AccountID>,
    ) -> Result<Vec<Account>, GetAccountByIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccountByIdError::Internal)?;

        let accounts_search: Vec<_> = account_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();

        let account_rows = sqlx::query!(
            r#"
            SELECT
                id as "id: AccountID",
                account_name,
                email as "email?",
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at,
                is_admin,
                provider,
                provider_identity_key
            FROM accounts
            WHERE id = ANY($1)
            "#,
            &accounts_search
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
        .map_err(GetAccountByIdError::Internal)?;

        Ok(account_rows
            .into_iter()
            .map(|account_row| Account {
                id: account_row.id,
                account_name: AccountName::new_unchecked(&account_row.account_name),
                email: account_row.email,
                display_name: account_row.display_name,
                account_type: account_row.account_type,
                avatar_url: account_row.avatar_url,
                registered_at: account_row.registered_at,
                is_admin: account_row.is_admin,
                provider: account_row.provider,
                provider_identity_key: account_row.provider_identity_key,
            })
            .collect())
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

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT
                id as "id: AccountID",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at,
                is_admin,
                provider,
                provider_identity_key
            FROM accounts
            WHERE lower(account_name) = lower($1)
            "#,
            account_name.to_string()
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
                is_admin: account_row.is_admin,
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

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(account_name) = lower($1)
            "#,
            account_name.to_string()
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
impl PasswordHashRepository for PostgresAccountRepository {
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

        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_name, password_hash)
                VALUES ($1, $2)
            "#,
            account_name.to_string(),
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

        let maybe_password_row = sqlx::query!(
            r#"
            SELECT password_hash
              FROM accounts_passwords
              WHERE lower(account_name) = lower($1)
            "#,
            account_name.to_string(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(FindPasswordHashError::Internal)?;

        Ok(maybe_password_row.map(|password_row| password_row.password_hash))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
