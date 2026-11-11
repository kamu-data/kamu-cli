// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::InMemoryTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_resources_inmem::register_dependencies;
use kamu_resources_repo_tests::resource_controller_state_repository_test_suite as controller_state_repo_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_upsert_and_get_state,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_not_reconciled_predicate,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_deletion_pending_predicate,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        controller_state_repo_suite::test_status_changed_predicate_only_when_reacting_to_status,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_generation_change,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_status_generation_change,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_done_suppression_lifted_by_controller_revision_bump,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_not_before_suppresses_until_due,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_retry_under_status_predicate,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_other_controllers_and_schemas_ignored,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_limit_and_distinct_ids,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_next_deadline,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_next_deadline_ignores_deleted_resources,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = controller_state_repo_suite::test_delete_states_for_resource,
    harness = InMemoryResourceControllerStateRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryResourceControllerStateRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryResourceControllerStateRepositoryHarness {
    pub fn new() -> Self {
        Self {
            catalog: Self::catalog_builder().build(),
        }
    }

    pub fn new_transactional() -> Self {
        let mut catalog_builder = Self::catalog_builder();
        catalog_builder.add::<InMemoryTransactionManager>();
        Self {
            catalog: catalog_builder.build(),
        }
    }

    fn catalog_builder() -> CatalogBuilder {
        let mut catalog_builder = CatalogBuilder::new();
        register_dependencies(&mut catalog_builder);
        catalog_builder.add::<InMemoryAccountRepository>();
        catalog_builder
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
