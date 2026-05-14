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
use kamu_configuration_postgres::PostgresDatasetSecretSetBindingRepository;
use kamu_configuration_repo_tests::dataset_secret_set_binding_repository_test_suite as binding_repo;
use kamu_datasets_postgres::PostgresDatasetEntryRepository;
use kamu_resources_postgres::PostgresResourceRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_replace_and_list_bindings,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_replace_overwrites_previous_bindings,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_replace_rejects_duplicates,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_delete_bindings_for_dataset,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_list_bindings_empty_initially,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = binding_repo::test_delete_bindings_for_dataset_no_op,
    harness = PostgresDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetSecretSetBindingRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetSecretSetBindingRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetSecretSetBindingRepository>();
        catalog_builder.add::<PostgresResourceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
