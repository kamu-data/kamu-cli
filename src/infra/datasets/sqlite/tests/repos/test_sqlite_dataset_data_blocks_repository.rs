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
use kamu_datasets_repo_tests::dataset_data_blocks_repo;
use kamu_datasets_sqlite::{SqliteDatasetDataBlockRepository, SqliteDatasetEntryRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_has_data_blocks,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch_duplicate_sequence_number,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_get_data_block_by_hash,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_get_page_of_data_blocks,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_delete_data_blocks,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_data_blocks_repo::test_remove_dataset_entry_removes_data_blocks,
    harness = SqliteDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetDataBlockRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetDataBlockRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();

        catalog_builder.add::<SqliteAccountRepository>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetDataBlockRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
