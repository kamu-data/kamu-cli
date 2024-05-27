// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{run_transactional, PostgresTransactionManager};
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_postgres::PostgresAccountRepository;
use sqlx::PgPool;

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_missing_account_not_found(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_missing_account_not_found(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_cli_account(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_password_account(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_github_account(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_github_account(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_multiple_github_account(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog: Catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_multiple_github_account(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_account_without_email(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_insert_and_locate_account_without_email(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_password_account_id(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_password_account_id(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_password_account_email(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_password_account_email(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_github_account_id(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_github_account_id(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_github_account_name(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_github_account_name(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_github_account_provider_identity(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_github_account_provider_identity(&catalog).await;
        Ok(())
    })
    .await
    .unwrap();
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_duplicate_github_account_email(pg_pool: PgPool) {
    let harness = PostgresAccountRepositoryHarness::new(pg_pool);

    run_transactional(&harness.catalog, |catalog| async move {
        kamu_accounts_repo_tests::test_duplicate_github_account_email(&catalog).await;
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
