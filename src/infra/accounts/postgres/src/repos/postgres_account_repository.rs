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
use internal_error::ResultIntoInternal;

use crate::domain::{AccountModel, AccountOrigin, AccountRepository, AccountRepositoryError};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl PostgresAccountRepository {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError> {
        let mut tr = self.transaction.lock().await;

        sqlx::query!(
            r#"
            INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at)
                VALUES ($1, $2, $3, $4, ($5::text)::account_origin, $6)
            "#,
            account_model.id,
            account_model.email,
            account_model.account_name,
            account_model.display_name,
            account_model.origin as AccountOrigin,
            account_model.registered_at,
        )
        .execute(tr.connection_mut())
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

        let account_data = sqlx::query_as!(
            AccountModel,
            r#"
            SELECT id, email, account_name, display_name, origin as "origin: _", registered_at
              FROM accounts
              WHERE email = $1
            "#,
            email
        )
        .fetch_optional(tr.connection_mut())
        .await
        .int_err()
        .map_err(AccountRepositoryError::Internal)?;

        Ok(account_data)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
