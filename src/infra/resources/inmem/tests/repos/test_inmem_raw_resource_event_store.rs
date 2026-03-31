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
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use kamu_resources_repo_tests::resource_raw_event_store_test_suite as event_store_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_event_store_empty,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_save_and_get_events,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_events_isolated_by_query,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_get_all_events,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_events_filtered_by_kind,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_get_events_with_windowing,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = event_store_suite::test_concurrent_modification_rejected,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryRawResourceEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryRawResourceEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryResourceRepository>();
        catalog_builder.add::<InMemoryRawResourceEventStore>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
