// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseTransactionRunner, PostgresTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_accounts_postgres::{PostgresAccessTokenRepository, PostgresAccountRepository};
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_missing_access_token_not_found(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_missing_account_not_found(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_access_token(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_access_token(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_locate_multiple_access_tokens(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_insert_and_locate_multiple_access_tokens(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_mark_existing_access_token_revorked(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_existing_access_token_revoked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_mark_non_existing_access_token_revorked(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_mark_non_existing_access_token_revoked(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_find_account_by_active_token_id(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_find_account_by_active_token_id(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_create_duplicate_active_access_token(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_create_duplicate_active_access_token(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_create_duplicate_access_token_err(pg_pool: PgPool) {
    let harness = PostgresAccessTokenRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_accounts_repo_tests::test_create_duplicate_access_token_err(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresAccessTokenRepositoryHarness {
    catalog: Catalog,
}

impl PostgresAccessTokenRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresAccessTokenRepository>();
        catalog_builder.add::<PostgresAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
