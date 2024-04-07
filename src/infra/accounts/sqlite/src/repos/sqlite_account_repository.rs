// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{LazyTransactionRef, TransactionRefT};
use dill::{component, interface};
use internal_error::ResultIntoInternal;

use crate::domain::{AccountModel, AccountRepository, AccountRepositoryError};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteAccountRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl SqliteAccountRepository {
    pub fn new(transaction: LazyTransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for SqliteAccountRepository {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(AccountRepositoryError::Internal)?;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            "#,
            account_model.id,
            account_model.email,
            account_model.account_name,
            account_model.display_name,
            account_model.origin,
            account_model.registered_at,
        )
        .execute(connection_mut)
        .await
        .int_err()
        .map_err(AccountRepositoryError::Internal)?;

        Ok(())
    }

    async fn find_account_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountModel>, AccountRepositoryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(AccountRepositoryError::Internal)?;

        let account_data = sqlx::query_as!(
            AccountModel,
            r#"
            SELECT id, email, account_name, display_name, origin as "origin: _", registered_at as "registered_at: _"
              FROM accounts
              WHERE email = $1
            "#,
            email
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map_err(AccountRepositoryError::Internal)?;

        Ok(account_data)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
