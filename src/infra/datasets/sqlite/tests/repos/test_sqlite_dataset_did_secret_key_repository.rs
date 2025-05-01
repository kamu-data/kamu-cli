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
use kamu_datasets_repo_tests::dataset_did_secret_key_repo;
use kamu_datasets_sqlite::{SqliteDatasetDidSecretKeyRepository, SqliteDatasetEntryRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = dataset_did_secret_key_repo::test_insert_and_locate_dataset_did_secret_keys,
    harness = SqliteDatasetDidSecretKeyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteDatasetDidSecretKeyRepositoryHarness {
    catalog: Catalog,
}

impl SqliteDatasetDidSecretKeyRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteDatasetEntryRepository>();
        catalog_builder.add::<SqliteDatasetDidSecretKeyRepository>();
        catalog_builder.add::<SqliteAccountRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
