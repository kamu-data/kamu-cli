// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{run_transactional, SqliteTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_sqlite::SqliteAccountRepository;
use sqlx::SqlitePool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_no_password_stored(sqlite_pool: SqlitePool) {
    let harness = SqliteAccountRepositoryHarness::new(sqlite_pool);

    run_transactional([&harness.catalog], |[catalog]| async move {
        kamu_accounts_repo_tests::test_no_password_stored(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_store_couple_account_passwords(sqlite_pool: SqlitePool) {
    let harness = SqliteAccountRepositoryHarness::new(sqlite_pool);

    run_transactional([&harness.catalog], |[catalog]| async move {
        kamu_accounts_repo_tests::test_store_couple_account_passwords(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

struct SqliteAccountRepositoryHarness {
    catalog: Catalog,
}

impl SqliteAccountRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
