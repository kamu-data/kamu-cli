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
use kamu_datasets_postgres::{
    PostgresDatasetBlockQueryRepository,
    PostgresDatasetDataBlockRepository,
    PostgresDatasetEntryRepository,
    PostgresDatasetKeyBlockRepository,
};
use kamu_datasets_repo_tests::dataset_block_query_repo;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_block_query_repo::test_list_blocks_unfiltered,
    harness = PostgresDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_block_query_repo::test_list_blocks_by_event_types,
    harness = PostgresDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_block_query_repo::test_list_blocks_by_hash_prefix,
    harness = PostgresDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_block_query_repo::test_list_blocks_paginated,
    harness = PostgresDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_block_query_repo::test_list_blocks_of_unknown_dataset,
    harness = PostgresDatasetBlockQueryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetBlockQueryRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetBlockQueryRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetKeyBlockRepository>();
        catalog_builder.add::<PostgresDatasetDataBlockRepository>();
        catalog_builder.add::<PostgresDatasetBlockQueryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
