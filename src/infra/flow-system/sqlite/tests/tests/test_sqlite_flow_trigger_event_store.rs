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
use kamu_flow_system_sqlite::SqliteFlowTriggerEventStore;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_trigger_event_store::test_event_store_empty,
    harness = SqliteFlowTriggerEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_trigger_event_store::test_event_store_get_streams,
    harness = SqliteFlowTriggerEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_trigger_event_store::test_event_store_get_events_with_windowing,
    harness = SqliteFlowTriggerEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteFlowTriggerEventStoreHarness {
    catalog: Catalog,
}

impl SqliteFlowTriggerEventStoreHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteFlowTriggerEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
