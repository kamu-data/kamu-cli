// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::SqliteTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_sqlite::SqliteAccountRepository;
use kamu_datasets_repo_tests::dataset_env_var_repo;
use kamu_datasets_sqlite::{SqliteDatasetEntryRepository, SqliteDatasetEnvVarRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_missing_dataset_env_var_not_found,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_insert_and_get_dataset_env_var,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_insert_and_get_multiple_dataset_env_vars,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_delete_dataset_env_vars,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_delete_all_dataset_env_vars,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_env_var_repo::test_upsert_dataset_env_vars,
    harness = SqliteDatasetEnvVarRepositoryHarness
);

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

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetEnvVarRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
