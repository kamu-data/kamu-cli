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
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_resources_inmem::register_dependencies;
use kamu_resources_repo_tests::resource_raw_event_store_test_suite as event_store_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_event_store_empty,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_and_get_events,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_events_isolated_by_query,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_all_events,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_events_filtered_by_kind,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_events_with_windowing,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_concurrent_modification_rejected,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_get_events_multi,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi_rejects_concurrent_modification_atomically,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = event_store_suite::test_save_events_multi_rejects_empty_item,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = event_store_suite::test_rolled_back_save_leaves_no_events,
    harness = InMemoryRawResourceEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryRawResourceEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryRawResourceEventStoreHarness {
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
