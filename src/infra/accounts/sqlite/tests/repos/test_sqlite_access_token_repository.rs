// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionRunner, SqliteTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_accounts_sqlite::{SqliteAccessTokenRepository, SqliteAccountRepository};
use sqlx::SqlitePool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_missing_access_token_not_found(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_missing_access_token_not_found(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_locate_access_token(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_access_token(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_locate_multiple_access_tokens(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_mark_existing_access_token_revorked(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_existing_access_token_revorked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_mark_non_existing_access_token_revorked(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_non_existing_access_token_revorked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_find_account_by_active_token_id(sqlite_pool: SqlitePool) {
    let harness = SqliteAccessTokenRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_find_account_by_active_token_id(&catalog).await;
            Ok::<_, InternalError>(())
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
        // Initialize catalog with predefined Sqlite pool
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
