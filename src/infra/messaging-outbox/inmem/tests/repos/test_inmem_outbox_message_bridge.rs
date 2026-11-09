// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::InMemoryTransactionManager;
use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use dill::{Catalog, CatalogBuilder};
use kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use wakeup_listener::WakeupListenerMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = kamu_messaging_outbox_repo_tests::test_no_outbox_messages_initially,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = kamu_messaging_outbox_repo_tests::test_push_messages_from_several_producers,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = kamu_messaging_outbox_repo_tests::test_push_many_messages_and_read_parts,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = kamu_messaging_outbox_repo_tests::test_try_reading_above_max,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        kamu_messaging_outbox_repo_tests::test_reading_messages_above_max_with_multiple_producers,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = kamu_messaging_outbox_repo_tests::test_consumption_boundaries_monotonic_and_isolated,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = kamu_messaging_outbox_repo_tests::test_first_consumption_boundaries_in_concurrent_transactions,
    harness = InMemoryOutboxMessageBridgeHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryOutboxMessageBridgeHarness {
    catalog: Catalog,
}

impl InMemoryOutboxMessageBridgeHarness {
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
        catalog_builder.add::<InMemoryOutboxMessageBridge>();
        catalog_builder.add::<InMemoryWakeupHub>();
        catalog_builder.add::<WakeupListenerMetrics>();
        catalog_builder
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
