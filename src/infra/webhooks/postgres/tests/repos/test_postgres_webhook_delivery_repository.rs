// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_task_system_postgres::PostgresTaskEventStore;
use kamu_webhooks_postgres::*;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_no_webhook_deliveries_initially,
    harness = PostgresWebhooDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_success_response,
    harness = PostgresWebhooDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_save_webhook_delivery_start_and_failure_response,
    harness = PostgresWebhooDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_filter_webhook_deliveries_by_task_id,
    harness = PostgresWebhooDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_delivery_repository_test_suite::test_filter_webhook_deliveries_by_webhook_event_or_subscription_id,
    harness = PostgresWebhooDeliveryRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresWebhooDeliveryRepositoryHarness {
    catalog: Catalog,
}

impl PostgresWebhooDeliveryRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresTaskEventStore>();
        catalog_builder.add::<PostgresWebhookSubscriptionEventStore>();
        catalog_builder.add::<PostgresWebhookEventRepository>();
        catalog_builder.add::<PostgresWebhookDeliveryRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
