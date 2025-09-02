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
use kamu_webhooks_postgres::PostgresWebhookSubscriptionEventStore;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_no_events_initially,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_store_single_aggregate,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_store_multiple_aggregates,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_modifying_subscription,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_non_unique_labels,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_non_unique_labels_on_modify,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_empty_labels_allowed_as_duplicates,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_empty_labels_allowed_as_duplicates_on_modify,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_same_dataset_different_event_types,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_webhooks_repo_tests::webhook_subscription_event_store_test_suite::test_removed_subscription_filters_out,
    harness = PostgresWebhookSubscriptionEventStoreHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresWebhookSubscriptionEventStoreHarness {
    catalog: Catalog,
}

impl PostgresWebhookSubscriptionEventStoreHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresWebhookSubscriptionEventStore>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
