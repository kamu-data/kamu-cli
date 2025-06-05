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
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_repo_tests::dataset_entry_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_get_dataset_entry,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_stream_many_entries,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_get_multiple_entries,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_get_dataset_entry_by_name,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_get_dataset_entries_by_owner_id,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_try_save_duplicate_dataset_entry,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_try_save_dataset_entry_with_name_collision,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_try_set_same_dataset_name_for_another_owned_dataset_entry,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_update_dataset_entry_name,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_owner_of_entries_renamed,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_entry_repo::test_delete_dataset_entry,
    harness = InMemoryDatasetEntryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetEntryRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetEntryRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
