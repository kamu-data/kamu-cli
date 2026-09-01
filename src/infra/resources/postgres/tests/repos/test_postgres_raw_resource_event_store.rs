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
use kamu_resources_postgres::{PostgresRawResourceEventStore, PostgresResourceRepository};
use kamu_resources_repo_tests::resource_raw_event_store_test_suite as event_store_suite;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_event_store_empty,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_save_and_get_events,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_events_isolated_by_query,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_get_all_events,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_events_filtered_by_kind,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_get_events_with_windowing,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_concurrent_modification_rejected,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_get_events_multi,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_save_events_multi,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_save_events_multi_rejects_concurrent_modification_atomically,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = event_store_suite::test_save_events_multi_rejects_empty_item,
    harness = PostgresRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresRawResourceEventStoreHarness {
    catalog: Catalog,
}

impl PostgresRawResourceEventStoreHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        use database_common::PostgresTransactionManager;

        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceRepository>();
        catalog_builder.add::<PostgresRawResourceEventStore>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
