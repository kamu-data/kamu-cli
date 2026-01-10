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
use kamu_flow_system_postgres::PostgresFlowEventStore;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_empty_filters_distingush_dataset,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_status,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_flow_type,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_initiator_with_multiple_variants,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_combinations,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_and_status,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_filter_by_datasets_with_pagination,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_flow_pagination_with_filters,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_dataset_get_flow_initiators,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_unfiltered_system_flows,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_type,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_flow_status,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_filtered_by_initiator,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flows_complex_filter,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_system_flow_pagination_with_filters,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_unpaged,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_pagination,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_all_flows_filters,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_single_type_crud,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_dataset_multiple_types_crud,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_multiple_datasets_crud,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_pending_flow_system_flow_crud,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_event_store_concurrent_modification,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_at_different_stages_through_success_path,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_visibility_when_aborted_before_activation,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_flow_activation_on_multiple_flows,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_get_all_scope_pending_flows,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_event_store::test_get_flows_for_multiple_datasets,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_event_store::test_flow_through_retry_attempts,
    harness = PostgresFlowEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresFlowEventStoreHarness {
    catalog: Catalog,
}

impl PostgresFlowEventStoreHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresFlowEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
