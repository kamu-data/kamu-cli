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
use kamu_webhooks_sqlite::*;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_no_watermark_initially,
    harness = SqliteWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_set_and_overwrite_watermark,
    harness = SqliteWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_watermarks_are_per_subscription_and_event_type,
    harness = SqliteWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_remove_watermark_of_one_event_type,
    harness = SqliteWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_remove_watermarks_of_subscription,
    harness = SqliteWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteWebhookWatermarkRepositoryHarness {
    catalog: Catalog,
}

impl SqliteWebhookWatermarkRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteWebhookSubscriptionEventStore>();
        catalog_builder.add::<SqliteWebhookWatermarkRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
