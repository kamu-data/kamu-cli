// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::SqliteTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_task_system_sqlite::SqliteTaskEventStore;
use kamu_webhooks_sqlite::*;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_no_webhook_deliveries_initially,
    harness = SqliteWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_success_response,
    harness = SqliteWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_failure_response,
    harness = SqliteWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_filter_webhook_deliveries_by_task_id,
    harness = SqliteWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_filter_webhook_deliveries_by_webhook_event_or_subscription_id,
    harness = SqliteWebhookDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteWebhookDeliveryRepositoryHarness {
    catalog: Catalog,
}

impl SqliteWebhookDeliveryRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteTaskEventStore>();
        catalog_builder.add::<SqliteWebhookSubscriptionEventStore>();
        catalog_builder.add::<SqliteWebhookEventRepository>();
        catalog_builder.add::<SqliteWebhookDeliveryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
