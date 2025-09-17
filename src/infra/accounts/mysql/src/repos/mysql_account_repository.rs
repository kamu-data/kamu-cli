// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{
    PaginationOpts,
    TransactionRef,
    TransactionRefT,
    mysql_generate_placeholders_list,
};
use dill::{component, interface};
use email_utils::Email;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use sqlx::Row;
use sqlx::error::DatabaseError;
use sqlx::mysql::MySqlRow;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlAccountRepository {
    transaction: TransactionRefT<sqlx::MySql>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn ExpensiveAccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl MySqlAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    fn convert_unique_constraint_violation(&self, e: &dyn DatabaseError) -> AccountErrorDuplicate {
        let mysql_error_message = e.message();

        let account_field = if mysql_error_message.contains("for key 'PRIMARY'") {
            AccountDuplicateField::Id
        } else if mysql_error_message.contains("for key 'idx_accounts_name'") {
            AccountDuplicateField::Name
        } else if mysql_error_message.contains("for key 'idx_accounts_email'") {
            AccountDuplicateField::Email
        } else if mysql_error_message.contains("for key 'idx_accounts_provider_identity_key'") {
            AccountDuplicateField::ProviderIdentityKey
        } else {
            tracing::error!(
                error = ?e,
                error_msg = mysql_error_message,
                "Unexpected MySQL error"
            );
            AccountDuplicateField::Id
        };

        AccountErrorDuplicate { account_field }
    }

    fn map_account_row(account_row: &MySqlRow) -> Account {
        // Reason for this method: SQLX Mysql Enum decode error
        //                         https://github.com/launchbadge/sqlx/issues/1379
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
impl AccountRepository for MySqlAccountRepository {
    async fn save_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, provider, provider_identity_key)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
            account.id.to_string(),
            account.prepare_account_name_for_storage(),
            account.email.as_ref().to_ascii_lowercase(),
            account.display_name,
            account.account_type,
            account.avatar_url,
            account.registered_at,
            account.provider.to_string(),
            account.provider_identity_key.to_string(),
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

    async fn update_account(&self, updated_account: &Account) -> Result<(), UpdateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts SET
                account_name = ?,
                email = ?,
                display_name = ?,
                account_type = ?,
                avatar_url = ?,
                registered_at = ?,
                provider = ?,
                provider_identity_key = ?
            WHERE id = ?
            "#,
            updated_account.prepare_account_name_for_storage(),
            updated_account.email.as_ref().to_ascii_lowercase(),
            updated_account.display_name,
            updated_account.account_type as AccountType,
            updated_account.avatar_url,
            updated_account.registered_at,
            updated_account.provider.to_string(),
            updated_account.provider_identity_key.to_string(),
            updated_account.id.to_string(),
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
        use odf::metadata::AsStackString;

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts SET email = ? WHERE id = ?
            "#,
            new_email.as_ref(),
            account_id_stack.as_str(),
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
                registered_at,
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
        account_ids: &[&odf::AccountID],
    ) -> Result<Vec<Account>, GetAccountsByIdsError> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
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
            WHERE id IN
            "#,
        );
        query_builder.push_tuples(account_ids, |mut b, account_id| {
            b.push_bind(account_id.to_string());
        });
        query_builder.push("ORDER BY account_name");

        let mysql_rows = query_builder
            .build()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let accounts = mysql_rows.iter().map(Self::map_account_row).collect();

        Ok(accounts)
    }

    async fn get_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<Account, GetAccountByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
                registered_at,
                provider,
                provider_identity_key
            FROM accounts
            WHERE account_name = ?
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

    async fn get_accounts_by_names(
        &self,
        account_names: &[&odf::AccountName],
    ) -> Result<Vec<Account>, GetAccountsByNamesError> {
        if account_names.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::MySql>::new(
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
            WHERE account_name IN
            "#,
        );
        query_builder.push_tuples(account_names, |mut b, account_name| {
            b.push_bind(account_name.as_str());
        });
        query_builder.push("ORDER BY account_name");

        let mysql_rows = query_builder
            .build()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let accounts = mysql_rows.iter().map(Self::map_account_row).collect();

        Ok(accounts)
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
        email: &Email,
    ) -> Result<Option<odf::AccountID>, FindAccountIdByEmailError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::AccountID;
        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE email = ?
            "#,
            email.as_ref()
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
        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE account_name = ?
            "#,
            account_name.to_string()
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
                WHERE (account_name LIKE CONCAT('%',?,'%')
                    OR display_name LIKE CONCAT('%',?,'%'))
                  AND id NOT IN ({})
                ORDER BY account_name
                LIMIT ? OFFSET ?
                "#,
                mysql_generate_placeholders_list(filters.exclude_accounts_by_ids.len())
            );

            // ToDo replace it by macro once sqlx will support it
            // https://github.com/launchbadge/sqlx/blob/main/FAQ.md#how-can-i-do-a-select--where-foo-in--query
            let mut query = sqlx::query(&query_str)
                // NOTE: it's intentionally twice
                .bind(name_pattern)
                .bind(name_pattern);

            for excluded_account_id in filters.exclude_accounts_by_ids {
                query = query.bind(excluded_account_id.to_string());
            }

            query = query.bind(limit).bind(offset);

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
    ) -> Result<(), DeleteAccountByNameError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM accounts
            WHERE account_name = ?
            "#,
            account_name.as_str()
        )
        .execute(&mut *connection_mut)
        .await
        .int_err()?;

        if delete_result.rows_affected() > 0 {
            Ok(())
        } else {
            Err(DeleteAccountByNameError::NotFound(
                AccountNotFoundByNameError {
                    account_name: account_name.clone(),
                },
            ))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ExpensiveAccountRepository for MySqlAccountRepository {
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
                LIMIT ? OFFSET ?
                "#,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;

            while let Some(entry) = query_stream.try_next().await? {
                yield Ok(entry.into());
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl PasswordHashRepository for MySqlAccountRepository {
    async fn save_password_hash(
        &self,
        account_id: &odf::AccountID,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        // TODO: duplicates are prevented with unique indices, but handle error

        let account_id = account_id.as_stack_string();

        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_id, password_hash)
            VALUES (?, ?)
            "#,
            account_id.as_str(),
            password_hash
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn modify_password_hash(
        &self,
        account_id: &odf::AccountID,
        password_hash: String,
    ) -> Result<(), ModifyPasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;
        let account_id_stack = account_id.as_stack_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts_passwords set password_hash = ?
                WHERE account_id = ?
            "#,
            password_hash,
            account_id_stack.as_str()
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        if update_result.rows_affected() == 0 {
            return Err(ModifyPasswordHashError::AccountNotFound(
                AccountNotFoundByIdError {
                    account_id: account_id.clone(),
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

        let maybe_password_row = sqlx::query!(
            r#"
            SELECT password_hash
              FROM accounts_passwords
              JOIN accounts ON accounts_passwords.account_id = accounts.id
              WHERE account_name = ?
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
