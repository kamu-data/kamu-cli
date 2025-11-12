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
use kamu_datasets_repo_tests::dataset_statistics_repo;
use kamu_datasets_sqlite::{SqliteDatasetEntryRepository, SqliteDatasetStatisticsRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_statistics_repo::test_set_and_get_statistics,
    harness = SqliteDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_statistics_repo::test_overwrite_statistics,
    harness = SqliteDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_statistics_repo::test_multiple_datasets_statistics,
    harness = SqliteDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_statistics_repo::test_get_total_statistics,
    harness = SqliteDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_statistics_repo::test_remove_dataset_entry_removes_statistics,
    harness = SqliteDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetStatisticsRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetStatisticsRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined SQlite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetStatisticsRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
