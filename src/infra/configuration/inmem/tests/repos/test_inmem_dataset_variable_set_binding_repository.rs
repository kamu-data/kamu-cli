// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_configuration_inmem::InMemoryDatasetVariableSetBindingRepository;
use kamu_configuration_repo_tests::dataset_variable_set_binding_repository_test_suite as binding_repo;
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_resources_inmem::InMemoryResourceRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_replace_and_list_bindings,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_replace_overwrites_previous_bindings,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_replace_rejects_duplicates,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_delete_bindings_for_dataset,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_list_bindings_empty_initially,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = binding_repo::test_delete_bindings_for_dataset_no_op,
    harness = InMemoryDatasetVariableSetBindingRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetVariableSetBindingRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetVariableSetBindingRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetVariableSetBindingRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryResourceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
