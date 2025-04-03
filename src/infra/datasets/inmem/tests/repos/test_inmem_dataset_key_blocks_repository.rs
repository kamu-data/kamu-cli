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
use kamu_datasets_inmem::{InMemoryDatasetEntryRepository, InMemoryDatasetKeyBlockRepository};
use kamu_datasets_repo_tests::dataset_key_blocks_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_has_blocks,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_save_blocks_batch,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_save_blocks_batch_duplicate_sequence_number,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_find_latest_block_of_kind,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_find_blocks_of_single_kind_in_range,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_find_blocks_of_multiple_kinds_in_range,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_find_max_sequence_number,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_delete_blocks,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_key_blocks_repo::test_remove_dataset_entry_removes_key_blocks,
    harness = InMemoryDatasetKeyBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetKeyBlockRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetKeyBlockRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryDatasetKeyBlockRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
