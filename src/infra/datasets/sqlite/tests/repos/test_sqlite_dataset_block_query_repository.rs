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
use kamu_datasets_repo_tests::dataset_block_query_repo;
use kamu_datasets_sqlite::{
    SqliteDatasetBlockQueryRepository,
    SqliteDatasetDataBlockRepository,
    SqliteDatasetEntryRepository,
    SqliteDatasetKeyBlockRepository,
};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_block_query_repo::test_list_blocks_unfiltered,
    harness = SqliteDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_block_query_repo::test_list_blocks_by_event_types,
    harness = SqliteDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_block_query_repo::test_list_blocks_by_hash_prefix,
    harness = SqliteDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_block_query_repo::test_list_blocks_paginated,
    harness = SqliteDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_block_query_repo::test_list_blocks_of_unknown_dataset,
    harness = SqliteDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetBlockQueryRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetBlockQueryRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetKeyBlockRepository>();
        catalog_builder.add::<SqliteDatasetDataBlockRepository>();
        catalog_builder.add::<SqliteDatasetBlockQueryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
