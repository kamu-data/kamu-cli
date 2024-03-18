// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::models::AccountModel;
use dill::{component, interface};
use kamu_core::auth::{AccountRepository, AccountRepositoryError};
use kamu_core::ResultIntoInternal;

use crate::PostgresConnectionPool;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresAccountRepository {
    pg_connection_pool: Arc<PostgresConnectionPool>,
}

#[component(pub)]
#[interface(dyn AccountRepository)]
impl PostgresAccountRepository {
    pub fn new(pg_connection_pool: Arc<PostgresConnectionPool>) -> Self {
        Self { pg_connection_pool }
    }
}

#[async_trait::async_trait]
impl AccountRepository for PostgresAccountRepository {
    async fn find_account_by_email(
        &self,
        email: &str,
    ) -> Result<Option<opendatafabric::AccountID>, AccountRepositoryError> {
        let mut pg_transaction = self
            .pg_connection_pool
            .begin_transaction()
            .await
            .int_err()
            .map_err(AccountRepositoryError::Internal)?;

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

        Ok(account_data.map(|a| opendatafabric::AccountID::from(a.id.as_simple().to_string())))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
