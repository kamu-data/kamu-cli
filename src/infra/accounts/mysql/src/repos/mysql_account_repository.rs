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
use sqlx::Row;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlAccountRepository {
    transaction: TransactionRefT<sqlx::MySql>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl MySqlAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for MySqlAccountRepository {
    async fn create_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, is_admin, provider, provider_identity_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            account.id.to_string(),
            account.account_name.to_ascii_lowercase(),
            account.email.as_ref().map(|email| email.to_ascii_lowercase()),
            account.display_name,
            account.account_type,
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
                    let mysql_error_message = e.message();

                    let account_field = if mysql_error_message.contains("for key 'PRIMARY'") {
                        CreateAccountDuplicateField::Id
                    } else if mysql_error_message.contains("for key 'idx_accounts_name'") {
                        CreateAccountDuplicateField::Name
                    } else if mysql_error_message.contains("for key 'idx_accounts_email'") {
                        CreateAccountDuplicateField::Email
                    } else if mysql_error_message.contains("for key 'idx_accounts_provider_identity_key'") {
                        CreateAccountDuplicateField::ProviderIdentityKey
                    } else {
                        tracing::error!(
                            error = ?e,
                            error_msg = mysql_error_message,
                            "Unexpected MySQL error"
                        );
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

        let connection_mut = tr.connection_mut().await?;

        let maybe_account_row = sqlx::query_as!(
            AccountRowModel,
            r#"
            SELECT
                id as "id: _",
                account_name,
                email as "email?",
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at,
                is_admin as "is_admin: _",
                provider,
                provider_identity_key
            FROM accounts
            WHERE id = ?
            "#,
            account_id.to_string()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(account_row) = maybe_account_row {
            Ok(account_row.into())
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

        let connection_mut = tr.connection_mut().await?;

        let placeholders = account_ids.iter().map(|_| "?").collect::<Vec<_>>();
        let placeholders_str = if placeholders.is_empty() {
            // MySQL does not consider the “in ()” syntax correct, so we add NULL
            "NULL".to_string()
        } else {
            placeholders.join(", ")
        };

        let query_str = format!(
            r#"
                SELECT
                    id,
                    account_name,
                    email,
                    display_name,
                    account_type,
                    avatar_url,
                    registered_at,
                    is_admin,
                    provider,
                    provider_identity_key
                FROM accounts
                WHERE id IN ({placeholders_str})
                "#,
        );

        // ToDo replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let mut query = sqlx::query(&query_str);
        for account_id in &account_ids {
            query = query.bind(account_id.to_string());
        }

        let account_rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(account_rows
            .into_iter()
            .map(|account_row| Account {
                id: account_row.get_unchecked("id"),
                account_name: AccountName::new_unchecked(
                    &account_row.get::<String, &str>("account_name"),
                ),
                email: account_row.get("email"),
                display_name: account_row.get("display_name"),
                account_type: account_row.get_unchecked("account_type"),
                avatar_url: account_row.get("avatar_url"),
                registered_at: account_row.get("registered_at"),
                is_admin: account_row.get::<bool, &str>("is_admin"),
                provider: account_row.get("provider"),
                provider_identity_key: account_row.get("provider_identity_key"),
            })
            .collect())
    }

    async fn get_account_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Account, GetAccountByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_account_row = sqlx::query_as!(
            AccountRowModel,
            r#"
            SELECT
                id as "id: _",
                account_name,
                email as "email?",
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at,
                is_admin as "is_admin: _",
                provider,
                provider_identity_key
            FROM accounts
            WHERE lower(account_name) = lower(?)
            "#,
            account_name.to_string()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        if let Some(account_row) = maybe_account_row {
            Ok(account_row.into())
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

        let connection_mut = tr.connection_mut().await?;

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE provider_identity_key = ?
            "#,
            provider_identity_key
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountID>, FindAccountIdByEmailError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(email) = lower(?)
            "#,
            email
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &AccountName,
    ) -> Result<Option<AccountID>, FindAccountIdByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(account_name) = lower(?)
            "#,
            account_name.to_string()
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for MySqlAccountRepository {
    async fn save_password_hash(
        &self,
        account_name: &AccountName,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        // TODO: duplicates are prevented with unique indices, but handle error

        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_name, password_hash)
                VALUES (?, ?)
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

        let connection_mut = tr.connection_mut().await?;

        let maybe_password_row = sqlx::query!(
            r#"
            SELECT password_hash
              FROM accounts_passwords
              WHERE lower(account_name) = lower(?)
            "#,
            account_name.to_string(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_password_row.map(|password_row| password_row.password_hash))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
