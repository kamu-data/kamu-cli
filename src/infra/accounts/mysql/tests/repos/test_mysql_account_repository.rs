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
use kamu_accounts_mysql::MySqlAccountRepository;
use sqlx::MySqlPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_missing_account_not_found(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

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
async fn test_insert_and_locate_cli_account(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_password_account(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_insert_and_locate_github_account(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_github_account(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_insert_and_locate_multiple_github_account(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_multiple_github_account(&catalog)
                .await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_insert_and_locate_account_without_email(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_account_without_email(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_password_account_id(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_password_account_id(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_password_account_email(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_password_account_email(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_github_account_id(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_github_account_id(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_github_account_name(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_github_account_name(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_github_account_provider_identity(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_github_account_provider_identity(&catalog)
                .await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, mysql)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/mysql"))]
async fn test_duplicate_github_account_email(mysql_pool: MySqlPool) {
    let harness = MySqlAccountRepositoryHarness::new(mysql_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_duplicate_github_account_email(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MySqlAccountRepositoryHarness {
    catalog: Catalog,
}

impl MySqlAccountRepositoryHarness {
    pub fn new(mysql_pool: MySqlPool) -> Self {
        // Initialize catalog with predefined MySql pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
