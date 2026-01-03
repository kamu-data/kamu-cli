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
use kamu_flow_system_inmem::{InMemoryFlowEventStore, InMemoryFlowSystemEventBridge};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_empty_filters_distingush_dataset,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_status,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_flow_type,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator_with_multiple_variants,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_combinations,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_and_status,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_with_pagination,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination_with_filters,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_get_flow_initiators,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_unfiltered_system_flows,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_type,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_status,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_initiator,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_complex_filter,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination_with_filters,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_unpaged,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_pagination,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_filters,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_single_type_crud,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_multiple_types_crud,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_multiple_datasets_crud,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_system_flow_crud,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_event_store_concurrent_modification,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_at_different_stages_through_success_path,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_when_aborted_before_activation,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_on_multiple_flows,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_get_all_scope_pending_flows,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_get_flows_for_multiple_datasets,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_flow_through_retry_attempts,
    harness = InMemoryFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryFlowEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryFlowEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryFlowEventStore>();
        catalog_builder.add::<InMemoryFlowSystemEventBridge>();
        catalog_builder.add::<SystemTimeSourceDefault>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
