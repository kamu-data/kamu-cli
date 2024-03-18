// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::models::{AccountModel, AccountOrigin};
use database_common::{DatabaseConfiguration, DatabaseError};
use internal_error::{InternalError, ResultIntoInternal};
use uuid::Uuid;

use crate::PostgresConnectionPool;

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn postgres_dummy_test(
    db_configuration: &DatabaseConfiguration,
) -> Result<(), InternalError> {
    let db_connection_pool = PostgresConnectionPool::new(db_configuration).int_err()?;

    let mut db_transaction = db_connection_pool.begin_transaction().await.int_err()?;

    sqlx::query_as!(
        AccountModel,
        r#"
        INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at)
          VALUES ($1, $2, $3, $4, ($5::text)::account_origin, $6)
        "#,
        Uuid::new_v4(),
        "test@example.com",
        "test",
        "Test User",
        AccountOrigin::Cli as AccountOrigin,
        Utc::now()
    )
    .execute(&mut **db_transaction)
    .await
    .map_err(DatabaseError::SqlxError)
    .int_err()?;

    let res = sqlx::query_as!(
        AccountModel,
        r#"
        SELECT id, email, account_name, display_name, origin as "origin: _", registered_at FROM accounts
        "#
    ).fetch_all(&mut **db_transaction)
    .await
    .map_err(DatabaseError::SqlxError).int_err()?;

    println!("Accounts: {res:?}");

    db_transaction.commit().await.int_err()?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////
