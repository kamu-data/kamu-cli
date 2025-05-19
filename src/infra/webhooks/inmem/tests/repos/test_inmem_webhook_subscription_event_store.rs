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
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_no_events_initially,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_store_single_aggregate,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_store_multiple_aggregates,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_modifying_subscription,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_non_unique_labels,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_non_unique_labels_on_modify,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_same_dataset_different_event_types,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_removed_subscription_filters_out,
    harness = InMemoryWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryWebhookSubscriptionEventStoreHarness {
    catalog: Catalog,
}

impl InMemoryWebhookSubscriptionEventStoreHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryWebhookSubscriptionEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
