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
use kamu_datasets_inmem::{
    InMemoryDatasetBlockQueryRepository,
    InMemoryDatasetDataBlockRepository,
    InMemoryDatasetEntryRepository,
    InMemoryDatasetKeyBlockRepository,
};
use kamu_datasets_repo_tests::dataset_block_query_repo;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_block_query_repo::test_list_blocks_unfiltered,
    harness = InMemoryDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_block_query_repo::test_list_blocks_by_event_types,
    harness = InMemoryDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_block_query_repo::test_list_blocks_by_hash_prefix,
    harness = InMemoryDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_block_query_repo::test_list_blocks_paginated,
    harness = InMemoryDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = dataset_block_query_repo::test_list_blocks_of_unknown_dataset,
    harness = InMemoryDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryDatasetBlockQueryRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryDatasetBlockQueryRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder.add::<InMemoryDatasetEntryRepository>();
        catalog_builder.add::<InMemoryDatasetKeyBlockRepository>();
        catalog_builder.add::<InMemoryDatasetDataBlockRepository>();
        catalog_builder.add::<InMemoryDatasetBlockQueryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
