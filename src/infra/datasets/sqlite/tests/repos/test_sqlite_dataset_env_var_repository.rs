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
use kamu_datasets_sqlite::SqliteDatasetEnvVarRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_missing_dataset_env_var_not_found(sqlite_pool: SqlitePool) {
    let harness = SqliteDatasetEnvVarRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_missing_dataset_env_var_not_found(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_get_dataset_env_var(sqlite_pool: SqlitePool) {
    let harness = SqliteDatasetEnvVarRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_insert_and_get_dataset_env_var(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_insert_and_get_multiple_dataset_env_vars(sqlite_pool: SqlitePool) {
    let harness = SqliteDatasetEnvVarRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_insert_and_get_multiple_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_delete_dataset_env_vars(sqlite_pool: SqlitePool) {
    let harness = SqliteDatasetEnvVarRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_delete_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_modify_dataset_env_vars(sqlite_pool: SqlitePool) {
    let harness = SqliteDatasetEnvVarRepositoryHarness::new(sqlite_pool);

    DatabaseTransactionRunner::new(harness.catalog)
        .transactional(|catalog| async move {
            kamu_datasets_repo_tests::test_modify_dataset_env_vars(&catalog).await;
            Ok::<_, InternalError>(())
        })
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetEnvVarRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetEnvVarRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Sqlite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteDatasetEnvVarRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}
