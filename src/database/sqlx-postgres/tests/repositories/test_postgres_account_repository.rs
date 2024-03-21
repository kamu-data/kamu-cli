// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{SubsecRound, Utc};
use database_common::models::{AccountModel, AccountOrigin};
use database_common::run_transactional;
use database_sqlx_postgres::{PostgresAccountRepository, PostgresConnectionPool, PostgresPlugin};
use dill::{Catalog, CatalogBuilder, Component};
use kamu_core::auth::AccountRepository;
use sqlx::PgPool;
use uuid::Uuid;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(sqlx::test(migrations = "../migrations/postgres"))]
async fn test_missing_account_not_found(pg_pool: PgPool) {
    let harness = PostgresSqlxTestHarness::new(pg_pool);
    let account_repo = harness.account_repository();

    run_transactional(&harness.catalog, async move |_, mut transaction_subject| {
        let maybe_account = account_repo
            .find_account_by_email(&mut transaction_subject, "test@example.com")
            .await
            .unwrap();

        assert!(maybe_account.is_none());
        Ok(transaction_subject)
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(sqlx::test(migrations = "../migrations/postgres"))]
async fn test_insert_and_locate_account(pg_pool: PgPool) {
    let harness = PostgresSqlxTestHarness::new(pg_pool);
    let account_repo = harness.account_repository();

    run_transactional(&harness.catalog, async move |_, mut transaction_subject| {
        let account_model = AccountModel {
            id: Uuid::new_v4(),
            email: String::from("test@example.com"),
            account_name: String::from("wasya"),
            display_name: String::from("Wasya Pupkin"),
            origin: AccountOrigin::Cli,
            registered_at: Utc::now().round_subsecs(6),
        };

        account_repo
            .create_account(&mut transaction_subject, &account_model)
            .await
            .unwrap();

        let maybe_account = account_repo
            .find_account_by_email(&mut transaction_subject, "test@example.com")
            .await
            .unwrap();
        assert!(maybe_account.is_some());
        assert_eq!(maybe_account, Some(account_model));
        Ok(transaction_subject)
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PostgresSqlxTestHarness {
    catalog: Catalog,
}

impl PostgresSqlxTestHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<PostgresPlugin>();
        catalog_builder.add_builder(PostgresConnectionPool::builder().with_pg_pool(pg_pool));
        catalog_builder.add::<PostgresAccountRepository>();
        let catalog = catalog_builder.build();

        Self { catalog }
    }

    pub fn account_repository(&self) -> Arc<dyn AccountRepository> {
        self.catalog.get_one::<dyn AccountRepository>().unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
