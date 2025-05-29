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
use email_utils::Email;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use sqlx::error::DatabaseError;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
#[interface(dyn ExpensiveAccountRepository)]
#[interface(dyn PasswordHashRepository)]
impl PostgresAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    fn convert_unique_constraint_violation(&self, e: &dyn DatabaseError) -> AccountErrorDuplicate {
        let account_field: AccountDuplicateField = match e.constraint() {
            Some("accounts_pkey") => AccountDuplicateField::Id,
            Some("idx_accounts_email") => AccountDuplicateField::Email,
            Some("idx_accounts_name") => AccountDuplicateField::Name,
            Some("idx_accounts_provider_identity_key") => {
                AccountDuplicateField::ProviderIdentityKey
            }
            _ => {
                tracing::error!(
                    error = ?e,
                    error_msg = e.message(),
                    "Unexpected Postgres error"
                );
                AccountDuplicateField::Id
            }
        };
        AccountErrorDuplicate { account_field }
    }
}

#[async_trait::async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn save_account(&self, account: &Account) -> Result<(), CreateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, account_name, email, display_name, account_type, avatar_url, registered_at, provider, provider_identity_key)
                VALUES ($1, $2, $3, $4, ($5::text)::account_type, $6, $7, $8, $9)
            "#,
            account.id.to_string(),
            account.prepare_account_name_for_storage(),
            account.email.as_ref().to_ascii_lowercase(),
            account.display_name,
            account.account_type as AccountType,
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

    async fn update_account(&self, updated_account: Account) -> Result<(), UpdateAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

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
            updated_account.id.to_string(),
            updated_account.prepare_account_name_for_storage(),
            updated_account.email.as_ref().to_ascii_lowercase(),
            updated_account.display_name,
            updated_account.account_type as AccountType,
            updated_account.avatar_url,
            updated_account.registered_at,
            updated_account.provider.to_string(),
            updated_account.provider_identity_key.to_string(),
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

        use odf::metadata::AsStackString;

        let account_id_stack = account_id.as_stack_string();

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts SET email = $1 WHERE id = $2
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

        use odf::metadata::AsStackString;

        let account_id_stack = account_id.as_stack_string();

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
            WHERE id = $1
            "#,
            account_id_stack.as_str()
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

        let accounts_search: Vec<_> = account_ids
            .iter()
            .map(std::string::ToString::to_string)
            .collect();

        let account_rows = sqlx::query_as!(
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
            WHERE id = ANY($1)
            "#,
            &accounts_search
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(account_rows.into_iter().map(Into::into).collect())
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
            WHERE lower(account_name) = lower($1)
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

        use odf::AccountID;
        let maybe_account_row = sqlx::query!(
            r#"
            SELECT id as "id: AccountID"
              FROM accounts
              WHERE lower(email) = lower($1)
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
              WHERE lower(account_name) = lower($1)
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

            let excluded_account_ids = filters
                .exclude_accounts_by_ids
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>();

            let mut query_stream = sqlx::query_as!(
                AccountRowModel,
                r#"
                SELECT id           AS "id: _",
                       account_name,
                       email,
                       display_name,
                       account_type AS "account_type: AccountType",
                       avatar_url,
                       registered_at,
                       provider,
                       provider_identity_key
                FROM accounts
                WHERE (account_name ILIKE '%'||$1||'%'
                    OR display_name ILIKE '%'||$1||'%')
                  AND id != ALL($4)
                ORDER BY account_name
                LIMIT $2 OFFSET $3
                "#,
                name_pattern,
                limit,
                offset,
                &excluded_account_ids
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            use futures::TryStreamExt;

            while let Some(account_row) = query_stream.try_next().await? {
                yield Ok(account_row.into());
            }
        })
    }

    async fn delete_account_by_name(
        &self,
        account_name: &odf::AccountName,
    ) -> Result<(), DeleteAccountError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let delete_result = sqlx::query!(
            r#"
            DELETE
            FROM accounts
            WHERE account_name = $1
            "#,
            account_name.as_str()
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
impl ExpensiveAccountRepository for PostgresAccountRepository {
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

        Ok(usize::try_from(accounts_count.unwrap()).unwrap())
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
                SELECT id           AS "id: _",
                       account_name,
                       email,
                       display_name,
                       account_type AS "account_type: AccountType",
                       avatar_url,
                       registered_at,
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
impl PasswordHashRepository for PostgresAccountRepository {
    async fn save_password_hash(
        &self,
        account_id: &odf::AccountID,
        account_name: &odf::AccountName,
        password_hash: String,
    ) -> Result<(), SavePasswordHashError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        use odf::metadata::AsStackString;

        let account_id = account_id.as_stack_string();

        sqlx::query!(
            r#"
            INSERT INTO accounts_passwords (account_name, password_hash, account_id)
            VALUES ($1, $2, $3)
            "#,
            account_name.as_str(),
            password_hash,
            account_id.as_str()
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

        let update_result = sqlx::query!(
            r#"
            UPDATE accounts_passwords set password_hash=$1
                where account_name=$2
            "#,
            password_hash,
            account_name.to_string()
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
        .int_err()?;

        Ok(maybe_password_row.map(|password_row| password_row.password_hash))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
