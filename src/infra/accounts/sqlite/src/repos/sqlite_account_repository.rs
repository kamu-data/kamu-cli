// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::{
    PaginationOpts,
    TransactionRef,
    TransactionRefT,
    sqlite_generate_placeholders_list,
};
use dill::{component, interface};
use email_utils::Email;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use sqlx::Row;
use sqlx::error::DatabaseError;
use sqlx::sqlite::SqliteRow;

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

    fn convert_unique_constraint_violation(&self, e: &dyn DatabaseError) -> AccountErrorDuplicate {
        let sqlite_error_message = e.message();

        let account_field = if sqlite_error_message.contains("accounts.id") {
            AccountDuplicateField::Id
        } else if sqlite_error_message.contains("idx_accounts_name") {
            AccountDuplicateField::Name
        } else if sqlite_error_message.contains("idx_accounts_email") {
            AccountDuplicateField::Email
        } else if sqlite_error_message.contains("accounts.provider_identity_key") {
            AccountDuplicateField::ProviderIdentityKey
        } else {
            tracing::error!(
                error = ?e,
                error_msg = sqlite_error_message,
                "Unexpected SQLite error"
            );
            AccountDuplicateField::Id
        };

        AccountErrorDuplicate { account_field }
    }

    fn map_account_row(account_row: &SqliteRow) -> Account {
        Account {
            id: account_row.get(0),
            account_name: odf::AccountName::new_unchecked(account_row.get::<&str, _>(1)),
            email: Email::parse(account_row.get(2)).unwrap(),
            display_name: account_row.get(3),
            account_type: account_row.get_unchecked(4),
            avatar_url: account_row.get(5),
            registered_at: account_row.get(6),
            provider: account_row.get(7),
            provider_identity_key: account_row.get(8),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for SqliteAccountRepository {
    async fn save_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let account_id = account.id.to_string();
        let account_name = account.account_name.to_ascii_lowercase();
        let email = account.email.as_ref().to_ascii_lowercase();
        let provider = account.provider.to_string();
        let provider_identity_key = account.provider_identity_key.to_string();

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, provider, provider_identity_key)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            account_id,
            account_name,
            email,
            account.display_name,
            account.account_type,
            account.avatar_url,
            account.registered_at,
            provider,
            provider_identity_key
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    CreateAccountError::Duplicate(self.convert_unique_constraint_violation(e.as_ref()))
                } else {
                    CreateAccountError::Internal(e.int_err())
                }
            }
            _ => CreateAccountError::Internal(e.int_err())
        })?;

        Ok(())
    }

    async fn update_account(&self, updated_account: Account) -> Result<(), UpdateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let account_id = updated_account.id.to_string();
        let account_name = updated_account.account_name.to_ascii_lowercase();
        let email = updated_account.email.as_ref().to_ascii_lowercase();
        let provider = updated_account.provider.to_string();
        let provider_identity_key = updated_account.provider_identity_key.to_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts SET
                account_name = $2,
                email = $3,
                display_name = $4,
                account_type = $5,
                avatar_url = $6,
                registered_at = $7,
                provider = $8,
                provider_identity_key = $9
            WHERE id = $1
            "#,
            account_id,
            account_name,
            email,
            updated_account.display_name,
            updated_account.account_type,
            updated_account.avatar_url,
            updated_account.registered_at,
            provider,
            provider_identity_key
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    UpdateAccountError::Duplicate(
                        self.convert_unique_constraint_violation(e.as_ref()),
                    )
                } else {
                    UpdateAccountError::Internal(e.int_err())
                }
            }
            _ => UpdateAccountError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(UpdateAccountError::NotFound(AccountNotFoundByIdError {
                account_id: updated_account.id.clone(),
            }));
        }

        Ok(())
    }

    async fn update_account_email(
        &self,
        account_id: &odf::AccountID,
        new_email: Email,
    ) -> Result<(), UpdateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let new_email = new_email.as_ref().to_string();
        let account_id_str = account_id.to_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts SET email = $1 WHERE id = $2
            "#,
            new_email,
            account_id_str,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) => {
                if e.is_unique_violation() {
                    UpdateAccountError::Duplicate(
                        self.convert_unique_constraint_violation(e.as_ref()),
                    )
                } else {
                    UpdateAccountError::Internal(e.int_err())
                }
            }
            _ => UpdateAccountError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(UpdateAccountError::NotFound(AccountNotFoundByIdError {
                account_id: account_id.clone(),
            }));
        }

        Ok(())
    }

    async fn get_account_by_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Account, GetAccountByIdError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
                provider,
                provider_identity_key
            FROM accounts
            WHERE id = $1
            "#,
            account_id_str
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
        account_ids: &[odf::AccountID],
    ) -> Result<Vec<Account>, GetAccountByIdError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
                    provider,
                    provider_identity_key
                FROM accounts
                WHERE id IN ({})
                "#,
            sqlite_generate_placeholders_list(account_ids.len(), NonZeroUsize::new(1).unwrap())
        );

        // ToDo replace it by macro once sqlx will support it
        // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
        let mut query = sqlx::query(&query_str);
        for account_id in account_ids {
            query = query.bind(account_id.to_string());
        }

        let account_rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(account_rows.iter().map(Self::map_account_row).collect())
    }

    async fn get_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Account, GetAccountByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
                provider,
                provider_identity_key
            FROM accounts
            WHERE lower(account_name) = lower($1)
            "#,
            account_name_str
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
    ) -> Result<Option<odf::AccountID>, FindAccountIdByProviderIdentityKeyError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_email(
        &self,
        email: &Email,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByEmailError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let email_str = email.as_ref();

        use odf::AccountID;
        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(email) = lower($1)
            "#,
            email_str,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    async fn find_account_id_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
        .int_err()?;

        Ok(maybe_account_row.map(|account_row| account_row.id))
    }

    fn search_accounts_by_name_pattern<'a>(
        &'a self,
        name_pattern: &'a str,
        filters: SearchAccountsByNamePatternFilters,
        pagination: PaginationOpts,
    ) -> AccountPageStream<'a> {
        Box::pin(async_stream::stream! {
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT id,
                       account_name,
                       email,
                       display_name,
                       account_type,
                       avatar_url,
                       registered_at,
                       provider,
                       provider_identity_key
                FROM accounts
                WHERE (account_name LIKE '%'||$1||'%' COLLATE nocase
                    OR display_name LIKE '%'||$1||'%' COLLATE nocase)
                  AND id NOT IN ({})
                ORDER BY account_name
                LIMIT $2 OFFSET $3
                "#,
                sqlite_generate_placeholders_list(
                    filters.exclude_accounts_by_ids.len(),
                    NonZeroUsize::new(4).unwrap()
                )
            );

            // ToDo replace it by macro once sqlx will support it
            // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
            let mut query = sqlx::query(&query_str)
                .bind(name_pattern)
                .bind(limit)
                .bind(offset);

            for excluded_account_id in filters.exclude_accounts_by_ids {
                query = query.bind(excluded_account_id.to_string());
            }

            let mut query_stream = query
                .fetch(connection_mut)
                .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;

            while let Some(account_row) = query_stream.try_next().await? {
                yield Ok(Self::map_account_row(&account_row));
            }
        })
    }

    async fn delete_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), DeleteAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let account_name_str = account_name.as_str();

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM accounts
            WHERE account_name = $1
            "#,
            account_name_str
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(DeleteAccountError::NotFound(AccountNotFoundByNameError {
                account_name: account_name.clone(),
            }))
        }
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
        account_id: &odf::AccountID,
        account_name: &odf::AccountName,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        // TODO: duplicates are prevented with unique indices, but handle error

        let account_name = account_name.as_str();
        let account_id_stack = account_id.as_did_str().to_stack_string();
        let account_id = account_id_stack.as_str();

        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_name, password_hash, account_id)
            VALUES ($1, $2, $3)
            "#,
            account_name,
            password_hash,
            account_id
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn modify_password_hash(
        &self,
        account_name: &odf::AccountName,
        password_hash: String,
    ) -> Result<(), ModifyPasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let account_name_string = account_name.to_string();
        let update_result = sqlx::query!(
            r#"
            UPDATE accounts_passwords set password_hash=$1
                where account_name=$2
            "#,
            password_hash,
            account_name_string
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if update_result.rows_affected() == 0 {
            return Err(ModifyPasswordHashError::AccountNotFound(
                AccountNotFoundByNameError {
                    account_name: account_name.clone(),
                },
            ));
        }

        Ok(())
    }

    async fn find_password_hash_by_account_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Option<String>, FindPasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
        .int_err()?;

        Ok(maybe_password_row.map(|password_row| password_row.password_hash))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
