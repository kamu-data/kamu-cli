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
use opendatafabric::AccountID;

use crate::domain::{AccountModel, AccountOrigin, AccountRepository, AccountRepositoryError};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlAccountRepository {
    transaction: TransactionRefT<sqlx::MySql>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl MySqlAccountRepository {
    pub fn new(transaction: LazyTransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

#[async_trait::async_trait]
impl AccountRepository for MySqlAccountRepository {
    async fn create_account(
        &self,
        account_model: &AccountModel,
    ) -> Result<(), AccountRepositoryError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(AccountRepositoryError::Internal)?;

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
            SELECT id as "id: AccountID", email, account_name, display_name, origin as "origin: AccountOrigin", registered_at
              FROM accounts
              WHERE email = ?
            "#,
            email
        ).fetch_optional(connection_mut)
            .await
            .int_err()
            .map_err(AccountRepositoryError::Internal)?;

        Ok(account_data)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
