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
use kamu_flow_system_postgres::{
    PostgresFlowProcessStateQuery,
    PostgresFlowProcessStateRepository,
};
use sqlx::PgPool;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_empty_state_table_initially,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_initial_state,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_immediate_stop,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_failing_state,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_recovery,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_pause,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_unfiltered,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_scope,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_flow_type,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_effective_status,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_combined_filters,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_from_csv_unfiltered_with_default_ordering,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_attempt_at_nulls_last,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_next_planned_at_nulls_last,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_failure_at_nulls_last,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_consecutive_failures,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_effective_state,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_name_alpha,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_flow_type,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_edge_cases,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_with_filters,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_different_orderings,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_boundary_conditions,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_flow_types,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_effective_states,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_attempt_between,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_failure_since,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_before,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_after,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_consecutive_failures,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_name_contains,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_scope,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_combined_filters,
    harness = PostgresFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresFlowProcessStateHarness {
    catalog: Catalog,
}

impl PostgresFlowProcessStateHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresFlowProcessStateQuery>();
        catalog_builder.add::<PostgresFlowProcessStateRepository>();
        catalog_builder.add::<SystemTimeSourceDefault>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
