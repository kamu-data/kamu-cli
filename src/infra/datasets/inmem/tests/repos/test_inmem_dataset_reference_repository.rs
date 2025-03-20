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
use kamu_datasets_inmem::{InMemoryDatasetEntryRepository, InMemoryDatasetReferenceRepository};
use kamu_datasets_repo_tests::dataset_reference_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_set_initial_reference,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_update_reference_without_cas_violation,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_update_reference_provoke_cas_violation,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_set_and_remove_reference,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_multiple_datasets,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_reference_repo::test_reacts_to_dataset_removals,
    harness = InMemoryDatasetReferenceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetReferenceRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetReferenceRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryDatasetReferenceRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
