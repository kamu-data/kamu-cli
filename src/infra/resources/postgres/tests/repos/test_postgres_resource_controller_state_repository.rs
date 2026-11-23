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
use kamu_resources_postgres::{
    PostgresResourceControllerStateRepository,
    PostgresResourceRepository,
};
use kamu_resources_repo_tests::resource_controller_state_repository_test_suite as controller_state_repo_suite;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_upsert_and_get_state,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_not_reconciled_predicate,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_deletion_pending_predicate,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        controller_state_repo_suite::test_status_changed_predicate_only_when_reacting_to_status,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_generation_change,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_status_generation_change,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_controller_revision_bump,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_not_before_suppresses_until_due,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_retry_under_status_predicate,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_other_controllers_and_schemas_ignored,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_limit_and_distinct_ids,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_next_deadline,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_next_deadline_ignores_deleted_resources,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_count_failing_resources,
    harness = PostgresResourceControllerStateRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_upsert_state_with_nul_in_error,
    harness = PostgresResourceControllerStateRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_upsert_state_of_unknown_resource_fails,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = controller_state_repo_suite::test_delete_states_for_resource,
    harness = PostgresResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresResourceControllerStateRepositoryHarness {
    catalog: Catalog,
}

impl PostgresResourceControllerStateRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceRepository>();
        catalog_builder.add::<PostgresResourceControllerStateRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
