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
use kamu_resources_repo_tests::resource_raw_event_store_test_suite as event_store_suite;
use kamu_resources_sqlite::{SqliteResourceRawEventStore, SqliteResourceRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_event_store_empty,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_save_and_get_events,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_events_isolated_by_query,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_get_all_events,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_events_filtered_by_kind,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_get_events_with_windowing,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = event_store_suite::test_concurrent_modification_rejected,
    harness = SqliteResourceRawEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteResourceRawEventStoreHarness {
    catalog: Catalog,
}

impl SqliteResourceRawEventStoreHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        use database_common::SqliteTransactionManager;

        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteResourceRepository>();
        catalog_builder.add::<SqliteResourceRawEventStore>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
