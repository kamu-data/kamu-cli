// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use kamu_resources_repo_tests::resource_raw_event_store_test_suite as event_store_suite;

use super::InMemoryResourceRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_event_store_empty,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_and_get_events,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_events_isolated_by_query,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_all_events,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_events_filtered_by_kind,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_events_with_windowing,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_concurrent_modification_rejected,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_events_multi,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi_rejects_concurrent_modification_atomically,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi_rejects_empty_item,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = event_store_suite::test_rolled_back_save_leaves_no_events,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
