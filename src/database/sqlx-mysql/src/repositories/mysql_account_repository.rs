// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::models::{AccountModel, AccountOrigin};
use database_common::TransactionSubject;
use dill::{component, interface};
use kamu_core::auth::{AccountRepository, AccountRepositoryError};
use kamu_core::ResultIntoInternal;

use crate::MySqlTransaction;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlAccountRepository {}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl MySqlAccountRepository {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait::async_trait]
impl AccountRepository for MySqlAccountRepository {
    async fn find_account_by_email(
        &self,
        transaction_subject: &mut TransactionSubject,
        email: &str,
    ) -> Result<Option<opendatafabric::AccountID>, AccountRepositoryError> {
        let mysql_transaction = transaction_subject
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

        Ok(account_data.map(|a| opendatafabric::AccountID::from(a.id.as_simple().to_string())))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
