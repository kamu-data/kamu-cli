// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{postgres::PostgresConnectionPool, mysql::MySQLConnectionPool, DatabaseConfiguration, DatabaseTransactionError};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize, sqlx::Type)]
#[sqlx(type_name = "account_origin", rename_all = "lowercase")]
enum AccountOrigin {
    Cli,
    Github,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct AccountModel {
    id: Uuid,
    email: String,
    account_name: String,
    display_name: String,
    origin: AccountOrigin,
    registered_at: DateTime<Utc>,
}


pub async fn dummy_database_test() -> Result<(), InternalError> {
    // let pg_db_configuration = DatabaseConfiguration::local_postgres();
    // dummy_postgres_test(&pg_db_configuration).await?;

    let mysql_db_configuration =  DatabaseConfiguration::local_mariadb();
    dummy_mysql_db_test(&mysql_db_configuration).await?;

    Ok(())
}
/*
async fn dummy_postgres_test(db_configuration: &DatabaseConfiguration) -> Result<(), InternalError> {
    let db_connection_pool = PostgresConnectionPool::new(db_configuration).int_err()?;

    let mut db_transaction = db_connection_pool
        .begin_transaction()
        .await
        .int_err()?;


    sqlx::query_as!(
        AccountModel,
        r#"INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at) 
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
    .map_err(DatabaseTransactionError::SqlxError).int_err()?;

    let res = sqlx::query_as!(
        AccountModel,
        r#"
        SELECT id, email, account_name, display_name, origin as "origin: _", registered_at FROM accounts
        "#
    ).fetch_all(&mut **db_transaction)
    .await
    .map_err(DatabaseTransactionError::SqlxError).int_err()?;

    println!("Accounts: {res:?}");

    db_transaction
        .commit()
        .await
        .int_err()?;

    Ok(())
}
*/
async fn dummy_mysql_db_test(db_configuration: &DatabaseConfiguration) -> Result<(), InternalError> {
    let db_connection_pool = MySQLConnectionPool::new(db_configuration).int_err()?;

    let mut db_transaction = db_connection_pool
        .begin_transaction()
        .await
        .int_err()?;


    sqlx::query_as!(
        AccountModel,
        r#"INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at) 
           VALUES (?, ?, ?, ?, ?, ?)
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
    .map_err(DatabaseTransactionError::SqlxError).int_err()?;

    let res = sqlx::query_as!(
        AccountModel,
        r#"
        SELECT id as "id: uuid::fmt::Hyphenated", email, account_name, display_name, origin as "origin: AccountOrigin", registered_at FROM accounts
        "#
    ).fetch_all(&mut **db_transaction)
    .await
    .map_err(DatabaseTransactionError::SqlxError).int_err()?;

    println!("Accounts: {res:?}");

    db_transaction
        .commit()
        .await
        .int_err()?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////
