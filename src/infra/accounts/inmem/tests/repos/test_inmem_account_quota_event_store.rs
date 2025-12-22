// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountQuotaEventStore;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_quota_added_and_fetched,
    harness = InMemoryAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_quota_modified,
    harness = InMemoryAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_quota_removed,
    harness = InMemoryAccountQuotaEventStoreHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_accounts_repo_tests::test_concurrent_modification_rejected,
    harness = InMemoryAccountQuotaEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryAccountQuotaEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryAccountQuotaEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder
            .add_value(InMemoryAccountQuotaEventStore::new())
            .bind::<dyn kamu_accounts::AccountQuotaEventStore, InMemoryAccountQuotaEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
