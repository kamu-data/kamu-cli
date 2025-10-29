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
use kamu_datasets_postgres::{PostgresDatasetDataBlockRepository, PostgresDatasetEntryRepository};
use kamu_datasets_repo_tests::dataset_data_blocks_repo;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_has_data_blocks,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_save_data_blocks_batch_duplicate_sequence_number,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_get_data_block_by_hash,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_get_page_of_data_blocks,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_delete_data_blocks,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_data_blocks_repo::test_remove_dataset_entry_removes_data_blocks,
    harness = PostgresDatasetDataBlockRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetDataBlockRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetDataBlockRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetDataBlockRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
