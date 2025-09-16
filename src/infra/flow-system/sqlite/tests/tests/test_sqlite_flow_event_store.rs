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
use kamu_flow_system_sqlite::SqliteFlowEventStore;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_empty_filters_distingush_dataset,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_status,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_flow_type,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator_with_multiple_variants,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_combinations,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_and_status,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_with_pagination,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination_with_filters,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_get_flow_initiators,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_unfiltered_system_flows,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_type,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_status,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_initiator,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_complex_filter,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination_with_filters,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_unpaged,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_pagination,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_filters,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_single_type_crud,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_multiple_types_crud,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_multiple_datasets_crud,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_system_flow_crud,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_event_store_concurrent_modification,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_at_different_stages_through_success_path,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_when_aborted_before_activation,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_multiple_flows,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_get_all_scope_pending_flows,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_get_flows_for_multiple_datasets,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_flow_through_retry_attempts,
    harness = SqliteFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteFlowEventStoreHarness {
    catalog: Catalog,
}

impl SqliteFlowEventStoreHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteFlowEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
