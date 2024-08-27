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
use kamu_task_system_inmem::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_task_system_repo_tests::test_event_store_empty,
    harness = InMemoryTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_task_system_repo_tests::test_event_store_get_streams,
    harness = InMemoryTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_with_windowing,
    harness = InMemoryTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_task_system_repo_tests::test_event_store_get_events_by_tasks,
    harness = InMemoryTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_task_system_repo_tests::test_event_store_get_dataset_tasks,
    harness = InMemoryTaskSystemEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryTaskSystemEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryTaskSystemEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryTaskSystemEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
