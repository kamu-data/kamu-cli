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
use kamu_task_system_inmem::InMemoryTaskEventStore;
use kamu_webhooks_inmem::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_no_webhook_deliveries_initially,
    harness = InMemoryWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_success_response,
    harness = InMemoryWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_failure_response,
    harness = InMemoryWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_filter_webhook_deliveries_by_webhook_event_or_subscription_id,
    harness = InMemoryWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryWebhookDeliveryRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryWebhookDeliveryRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryTaskEventStore>();
        catalog_builder.add::<InMemoryWebhookSubscriptionEventStore>();
        catalog_builder.add::<InMemoryWebhookEventRepository>();
        catalog_builder.add::<InMemoryWebhookDeliveryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
