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
use kamu_configuration_repo_tests::dataset_secret_set_binding_repository_test_suite as binding_repo;
use kamu_configuration_sqlite::SqliteDatasetSecretSetBindingRepository;
use kamu_datasets_sqlite::SqliteDatasetEntryRepository;
use kamu_resources_sqlite::SqliteResourceRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_replace_and_list_bindings,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_replace_overwrites_previous_bindings,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_replace_rejects_duplicates,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_delete_bindings_for_dataset,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_list_bindings_empty_initially,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = binding_repo::test_delete_bindings_for_dataset_no_op,
    harness = SqliteDatasetSecretSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetSecretSetBindingRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetSecretSetBindingRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetSecretSetBindingRepository>();
        catalog_builder.add::<SqliteResourceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
