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
use kamu_task_system_sqlite::SqliteTaskEventStore;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_empty,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_get_streams,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_with_windowing,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_by_tasks,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_get_dataset_tasks,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_try_get_queued_single_task,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_try_get_queued_multiple_tasks,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_get_running_tasks,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_task_system_repo_tests::test_event_store_concurrent_modification,
    harness = SqliteTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteTaskSystemEventStoreHarness {
    catalog: Catalog,
}

impl SqliteTaskSystemEventStoreHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined SQLite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteTaskEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
