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
use dill::CatalogBuilder;
use internal_error::{InternalError, ResultIntoInternal};
use uuid::Uuid;

use crate::{MySQLCatalogInitializer, MySQLConnectionPool};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn dummy_mysql_db_test(
    db_configuration: &DatabaseConfiguration,
) -> Result<(), InternalError> {
    let mut catalog_builder = CatalogBuilder::new();
    MySQLCatalogInitializer::init_database_components(&mut catalog_builder, db_configuration)
        .int_err()?;
    let mysql_catalog = catalog_builder.build();

    let db_connection_pool = mysql_catalog.get_one::<MySQLConnectionPool>().unwrap();

    let mut db_transaction = db_connection_pool.begin_transaction().await.int_err()?;

    sqlx::query_as!(
        AccountModel,
        r#"
        INSERT INTO accounts (id, email, account_name, display_name, origin, registered_at)
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
    .map_err(DatabaseError::SqlxError)
    .int_err()?;

    let res = sqlx::query_as!(
        AccountModel,
        r#"
        SELECT id as "id: uuid::fmt::Hyphenated", email, account_name, display_name, origin as "origin: AccountOrigin", registered_at FROM accounts
        "#
    ).fetch_all(&mut **db_transaction)
    .await
    .map_err(DatabaseError::SqlxError).int_err()?;

    println!("Accounts: {res:?}");

    db_transaction.commit().await.int_err()?;

    Ok(())
}

/////////////////////////////////////////////////////////////////////////////////////////
