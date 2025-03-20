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
use kamu_datasets_postgres::{PostgresDatasetEntryRepository, PostgresDatasetReferenceRepository};
use kamu_datasets_repo_tests::dataset_reference_repo;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_set_initial_reference,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_update_reference_without_cas_violation,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_update_reference_provoke_cas_violation,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_set_and_remove_reference,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_multiple_datasets,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_reference_repo::test_reacts_to_dataset_removals,
    harness = PostgresDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetReferenceRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetReferenceRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetReferenceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
