// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use kamu_resources_repo_tests::resource_label_projection_repository_test_suite as label_projection_repo_suite;

use super::InMemoryResourceRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_no_entries_initially,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_then_find,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_overwrites_previous_set,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_replace_entries_with_empty_slice_clears,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = label_projection_repo_suite::test_entries_isolated_by_resource_id,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = label_projection_repo_suite::test_rolled_back_replace_keeps_previous_entries,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
