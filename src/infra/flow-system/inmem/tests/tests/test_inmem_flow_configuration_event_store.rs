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
use kamu_flow_system_inmem::*;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_configuration_event_store::test_event_store_empty,
    harness = InMemoryFlowConfigurationEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_configuration_event_store::test_event_store_get_streams,
    harness = InMemoryFlowConfigurationEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_configuration_event_store::test_event_store_get_events_with_windowing,
    harness = InMemoryFlowConfigurationEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryFlowConfigurationEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryFlowConfigurationEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryFlowConfigurationEventStore>();
        catalog_builder.add::<InMemoryFlowSystemEventBridge>();
        catalog_builder.add::<SystemTimeSourceDefault>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
