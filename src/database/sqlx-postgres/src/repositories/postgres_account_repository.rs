// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::models::{AccountModel, AccountOrigin};
use database_common::Transaction;
use dill::{component, interface};
use kamu_core::auth::{AccountRepository, AccountRepositoryError};
use kamu_core::ResultIntoInternal;

use crate::PostgresTransaction;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountRepository {
    transaction_ptr: Arc<tokio::sync::Mutex<Transaction>>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl PostgresAccountRepository {
    pub fn new(transaction_ptr: Arc<tokio::sync::Mutex<Transaction>>) -> Self {
        Self { transaction_ptr }
    }
}

#[async_trait::async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError> {
        let mut transaction_guard = self.transaction_ptr.lock().await;

        let pg_transaction = transaction_guard
            .transaction
            .downcast_mut::<PostgresTransaction>()
            .unwrap();

        sqlx::query_as!(
            AccountModel,
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
        .execute(&mut **pg_transaction)
        .await
        .int_err()
        .map_err(AccountRepositoryError::Internal)?;

        Ok(())
    }

    async fn find_account_by_email(
        &self,
        email: &str,
    ) -> Result<Option<AccountModel>, AccountRepositoryError> {
        let mut transaction_guard = self.transaction_ptr.lock().await;

        let pg_transaction = transaction_guard
            .transaction
            .downcast_mut::<PostgresTransaction>()
            .unwrap();

        let account_data = sqlx::query_as!(
            AccountModel,
            r#"
            SELECT id, email, account_name, display_name, origin as "origin: _", registered_at
              FROM accounts
              WHERE email = $1
            "#,
            email
        )
        .fetch_optional(&mut **pg_transaction)
        .await
        .int_err()
        .map_err(AccountRepositoryError::Internal)?;

        Ok(account_data)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
