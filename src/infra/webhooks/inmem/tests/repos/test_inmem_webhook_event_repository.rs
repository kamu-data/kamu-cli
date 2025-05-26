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
use kamu_webhooks_inmem::InMemoryWebhookEventRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_no_webhook_events_initially,
    harness = InMemoryWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_single_webhook_event,
    harness = InMemoryWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_multiple_webhook_events,
    harness = InMemoryWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_webhook_events_pagination,
    harness = InMemoryWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_webhooks_repo_tests::webhook_event_repository_test_suite::test_create_duplicate_webhook_events,
    harness = InMemoryWebhookEventRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryWebhookEventRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryWebhookEventRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryWebhookEventRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
