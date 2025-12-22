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
use kamu_accounts_postgres::PostgresAccountQuotaEventStore;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::test_quota_added_and_fetched,
    harness = PostgresAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::test_quota_modified,
    harness = PostgresAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::test_quota_removed,
    harness = PostgresAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_accounts_repo_tests::test_concurrent_modification_rejected,
    harness = PostgresAccountQuotaEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresAccountQuotaEventStoreHarness {
    catalog: Catalog,
}

impl PostgresAccountQuotaEventStoreHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresAccountQuotaEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
