// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use sqlx::Row;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteAccountRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn ExpensiveAccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl SqliteAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
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
                        tracing::error!(
                            error = ?e,
                            error_msg = sqlite_error_message,
                            "Unexpected SQLite error"
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
        account_id: &odf::AccountID,
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
                id as "id: _",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at as "registered_at: _",
                is_admin as "is_admin: _",
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
            Ok(account_row.into())
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
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(GetAccountByIdError::Internal)?;

        let placeholders = account_ids
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ");

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
                WHERE id IN ({placeholders})
                "#,
        );

        // ToDo replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let mut query = sqlx::query(&query_str);
        for account_id in &account_ids {
            query = query.bind(account_id.to_string());
        }

        let account_rows = query
            .fetch_all(connection_mut)
            .await
            .int_err()
            .map_err(GetAccountByIdError::Internal)?;

        Ok(account_rows
            .into_iter()
            .map(|account_row| Account {
                id: account_row.get_unchecked("id"),
                account_name: odf::AccountName::new_unchecked(
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
        account_name: &odf::AccountName,
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
                id as "id: _",
                account_name,
                email,
                display_name,
                account_type as "account_type: AccountType",
                avatar_url,
                registered_at as "registered_at: _",
                is_admin as "is_admin: _",
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
    ) -> Result<Option<odf::AccountID>, FindAccountIdByProviderIdentityKeyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByProviderIdentityKeyError::Internal)?;

        use odf::AccountID;
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
    ) -> Result<Option<odf::AccountID>, FindAccountIdByEmailError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByEmailError::Internal)?;

        use odf::AccountID;
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
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(FindAccountIdByNameError::Internal)?;

        use odf::AccountID;
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ExpensiveAccountRepository for SqliteAccountRepository {
    async fn accounts_count(&self) -> Result<usize, AccountsCountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let accounts_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*)
            FROM accounts
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(accounts_count).unwrap_or(0))
    }

    async fn get_accounts(&self, pagination: PaginationOpts) -> AccountPageStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query_as!(
                AccountRowModel,
                r#"
                SELECT id            AS "id: _",
                       account_name,
                       email,
                       display_name,
                       account_type  AS "account_type: AccountType",
                       avatar_url,
                       registered_at AS "registered_at: _",
                       is_admin      AS "is_admin: _",
                       provider,
                       provider_identity_key
                FROM accounts
                ORDER BY registered_at ASC
                LIMIT $1 OFFSET $2
                "#,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;

            while let Some(account_row_model) = query_stream.try_next().await? {
                yield Ok(account_row_model.into());
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for SqliteAccountRepository {
    async fn save_password_hash(
        &self,
        account_name: &odf::AccountName,
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
        account_name: &odf::AccountName,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
