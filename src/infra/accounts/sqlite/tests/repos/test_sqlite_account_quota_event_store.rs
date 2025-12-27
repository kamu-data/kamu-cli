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
use kamu_accounts_sqlite::SqliteAccountQuotaEventStore;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_accounts_repo_tests::test_quota_added_and_fetched,
    harness = SqliteAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_accounts_repo_tests::test_quota_modified,
    harness = SqliteAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_accounts_repo_tests::test_quota_removed,
    harness = SqliteAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_accounts_repo_tests::test_concurrent_modification_rejected,
    harness = SqliteAccountQuotaEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteAccountQuotaEventStoreHarness {
    catalog: Catalog,
}

impl SqliteAccountQuotaEventStoreHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteAccountQuotaEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
