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
use kamu_datasets_postgres::PostgresDatasetEnvVarRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_missing_dataset_env_var_not_found(pg_pool: PgPool) {
    let harness = PostgresDatasetEnvVarRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_missing_dataset_env_var_not_found(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_get_dataset_env_var(pg_pool: PgPool) {
    let harness = PostgresDatasetEnvVarRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_insert_and_get_dataset_env_var(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_insert_and_get_multiple_dataset_env_vars(pg_pool: PgPool) {
    let harness = PostgresDatasetEnvVarRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_insert_and_get_multiple_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_delete_dataset_env_vars(pg_pool: PgPool) {
    let harness = PostgresDatasetEnvVarRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_delete_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_modify_dataset_env_vars(pg_pool: PgPool) {
    let harness = PostgresDatasetEnvVarRepositoryHarness::new(pg_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_modify_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetEnvVarRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetEnvVarRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresDatasetEnvVarRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
