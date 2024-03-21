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
use database_common::TransactionSubject;
use dill::{component, interface};
use kamu_core::auth::{AccountRepository, AccountRepositoryError};
use kamu_core::ResultIntoInternal;

use crate::MySqlTransaction;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlAccountRepository {
    transaction_ptr: Arc<tokio::sync::Mutex<TransactionSubject>>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl MySqlAccountRepository {
    pub fn new(transaction_ptr: Arc<tokio::sync::Mutex<TransactionSubject>>) -> Self {
        Self { transaction_ptr }
    }
}

#[async_trait::async_trait]
impl AccountRepository for MySqlAccountRepository {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError> {
        let mut transaction_guard = self.transaction_ptr.lock().await;

        let mysql_transaction = transaction_guard
            .transaction
            .downcast_mut::<MySqlTransaction>()
            .unwrap();

        sqlx::query_as!(
            AccountModel,
            r#"
            INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at)
                VALUES (?, ?, ?, ?, ?, ?)
            "#,
            account_model.id,
            account_model.email,
            account_model.account_name,
            account_model.display_name,
            account_model.origin as AccountOrigin,
            account_model.registered_at,
        )
        .execute(&mut **mysql_transaction)
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

        let mysql_transaction = transaction_guard
            .transaction
            .downcast_mut::<MySqlTransaction>()
            .unwrap();

        let account_data = sqlx::query_as!(
            AccountModel,
            r#"
            SELECT id as "id: uuid::fmt::Hyphenated", email, account_name, display_name, origin as "origin: AccountOrigin", registered_at
              FROM accounts
              WHERE email = ?
            "#,
            email
        ).fetch_optional(&mut **mysql_transaction)
            .await
            .int_err()
            .map_err(AccountRepositoryError::Internal)?;

        Ok(account_data)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
