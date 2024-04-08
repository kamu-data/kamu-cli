// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{SubsecRound, Utc};
use database_common::{run_transactional, PostgresTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_accounts::{AccountModel, AccountOrigin, AccountRepository};
use kamu_accounts_postgres::PostgresAccountRepository;
use sqlx::PgPool;
use uuid::Uuid;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_missing_account_not_found(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

        let maybe_account = account_repo
            .find_account_by_email("test@example.com")
            .await
            .unwrap();

        assert!(maybe_account.is_none());
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_account(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        let account_model = AccountModel {
            id: Uuid::new_v4().to_string(),
            email: String::from("test@example.com"),
            account_name: String::from("wasya"),
            display_name: String::from("Wasya Pupkin"),
            origin: AccountOrigin::Cli,
            registered_at: Utc::now().round_subsecs(6),
        };

        let account_repo = catalog.get_one::<dyn AccountRepository>().unwrap();

        account_repo.create_account(&account_model).await.unwrap();

        let maybe_account = account_repo
            .find_account_by_email("test@example.com")
            .await
            .unwrap();
        assert!(maybe_account.is_some());
        assert_eq!(maybe_account, Some(account_model));

        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

struct PostgresAccountRepositoryHarness {
    catalog: Catalog,
}

impl PostgresAccountRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
