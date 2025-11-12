// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_postgres::PostgresAccountRepository;
use kamu_datasets_postgres::{PostgresDatasetEntryRepository, PostgresDatasetStatisticsRepository};
use kamu_datasets_repo_tests::dataset_statistics_repo;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_statistics_repo::test_set_and_get_statistics,
    harness = PostgresDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_statistics_repo::test_overwrite_statistics,
    harness = PostgresDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_statistics_repo::test_multiple_datasets_statistics,
    harness = PostgresDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_statistics_repo::test_remove_dataset_entry_removes_statistics,
    harness = PostgresDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_statistics_repo::test_get_total_statistics,
    harness = PostgresDatasetStatisticsRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetStatisticsRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetStatisticsRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetStatisticsRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
