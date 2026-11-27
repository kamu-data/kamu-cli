// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use kamu_configuration_repo_tests::variable_set_projection_repository_test_suite as variable_set_repo;

use super::InMemoryConfigurationRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_entries_empty_initially,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_replace_and_get_entries,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_find_entry_by_key,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_entries_isolated_by_resource,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_entries_isolated_by_generation,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_get_latest_entries_before_generation,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_replace_entries_concurrent_modification,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_cleanup_entries_before_generation,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_cleanup_does_not_affect_other_resources,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_replace_preserves_stable_identity_and_creation_time,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_get_latest_entries,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_delete_all_entries,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = variable_set_repo::test_purging_the_resource_removes_its_entries,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = variable_set_repo::test_rolled_back_delete_all_entries_keeps_entries,
    harness = InMemoryConfigurationRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
