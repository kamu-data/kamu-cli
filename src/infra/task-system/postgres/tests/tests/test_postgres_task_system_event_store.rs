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
use kamu_task_system_postgres::PostgresTaskEventStore;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_empty,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_get_streams,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_with_windowing,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_by_tasks,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_get_dataset_tasks,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_try_get_queued_single_task,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_try_get_queued_multiple_tasks,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_task_system_repo_tests::test_event_store_get_running_tasks,
    harness = PostgresTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresTaskSystemEventStoreHarness {
    catalog: Catalog,
}

impl PostgresTaskSystemEventStoreHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresTaskEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
