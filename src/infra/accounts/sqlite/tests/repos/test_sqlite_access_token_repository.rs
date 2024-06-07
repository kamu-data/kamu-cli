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
use kamu_accounts_sqlite::{SqliteAccessTokenRepository, SqliteAccountRepository};
use sqlx::SqlitePool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_missing_access_token_not_found(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        kamu_accounts_repo_tests::test_missing_access_token_not_found(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_locate_access_token(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_access_token(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_locate_multiple_access_tokens(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

struct SqliteAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl SqliteAccessTokenRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteAccessTokenRepository>();
        catalog_builder.add::<SqliteAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
