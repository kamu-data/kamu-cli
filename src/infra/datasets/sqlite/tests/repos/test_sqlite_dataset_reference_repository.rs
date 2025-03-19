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
use kamu_datasets_repo_tests::dataset_reference_repo;
use kamu_datasets_sqlite::{SqliteDatasetEntryRepository, SqliteDatasetReferenceRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_set_initial_reference,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_update_reference_without_cas_violation,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_update_reference_provoke_cas_violation,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_set_and_remove_reference,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_multiple_datasets,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_reference_repo::test_reacts_to_dataset_removals,
    harness = SqliteDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetReferenceRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetReferenceRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Sqlite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetReferenceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
