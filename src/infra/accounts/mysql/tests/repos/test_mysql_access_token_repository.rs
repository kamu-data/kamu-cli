// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionRunner, MySqlTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_accounts_mysql::{MySqlAccessTokenRepository, MySqlAccountRepository};
use sqlx::MySqlPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_missing_access_token_not_found(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_missing_account_not_found(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_insert_and_locate_access_token(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_access_token(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_insert_and_locate_multiple_access_tokens(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_mark_existing_access_token_revorked(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_existing_access_token_revorked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_mark_non_existing_access_token_revorked(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_non_existing_access_token_revorked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_find_account_by_active_token_id(mysql_pool: MySqlPool) {
    let harness = MySqlAccessTokenRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_find_account_by_active_token_id(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MySqlAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl MySqlAccessTokenRepositoryHarness {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlAccessTokenRepository>();
        catalog_builder.add::<MySqlAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
