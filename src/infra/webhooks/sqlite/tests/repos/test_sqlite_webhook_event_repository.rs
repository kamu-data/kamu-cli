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
use kamu_webhooks_sqlite::SqliteWebhookEventRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_no_webhook_events_initially,
    harness = SqliteWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_single_webhook_event,
    harness = SqliteWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_multiple_webhook_events,
    harness = SqliteWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_duplicate_webhook_events,
    harness = SqliteWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_webhook_events_pagination,
    harness = SqliteWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteWebhookEventRepositoryHarness {
    catalog: Catalog,
}

impl SqliteWebhookEventRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteWebhookEventRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
