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
use database_common::{
    run_transactional,
    DatabaseConfiguration,
    DatabaseError,
    DatabaseTransactionManager,
    TransactionSubject,
};
use dill::{Catalog, CatalogBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use uuid::Uuid;

use crate::{PostgresPlugin, PostgresTransaction};

/////////////////////////////////////////////////////////////////////////////////////////

pub async fn postgres_dummy_test(
    db_configuration: &DatabaseConfiguration,
) -> Result<(), InternalError> {
    let mut catalog_builder = CatalogBuilder::new();
    PostgresPlugin::init_database_components(&mut catalog_builder, db_configuration).int_err()?;
    let base_catalog = catalog_builder.build();

    let db_transaction_manager = base_catalog
        .get_one::<dyn DatabaseTransactionManager>()
        .unwrap();

    run_transactional(
        db_transaction_manager.as_ref(),
        base_catalog,
        postgres_transaction_scenario,
    )
    .await
}

/////////////////////////////////////////////////////////////////////////////////////////

#[allow(clippy::no_effect_underscore_binding)]
async fn postgres_transaction_scenario(
    _catalog: Catalog,
    mut transaction_subject: TransactionSubject,
) -> Result<TransactionSubject, InternalError> {
    let pg_transaction = transaction_subject
        .transaction
        .downcast_mut::<PostgresTransaction>()
        .unwrap();

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
    .execute(&mut **pg_transaction)
    .await
    .map_err(DatabaseError::SqlxError)
    .int_err()?;

    let res = sqlx::query_as!(
        AccountModel,
        r#"
        SELECT id, email, account_name, display_name, origin as "origin: _", registered_at FROM accounts
        "#
    ).fetch_all(&mut **pg_transaction)
    .await
    .map_err(DatabaseError::SqlxError).int_err()?;

    println!("Accounts: {res:?}");

    Ok(transaction_subject)
}

/////////////////////////////////////////////////////////////////////////////////////////
