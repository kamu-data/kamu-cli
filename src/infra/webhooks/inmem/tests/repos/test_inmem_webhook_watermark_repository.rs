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
use kamu_webhooks_inmem::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_no_watermark_initially,
    harness = InMemoryWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_set_and_overwrite_watermark,
    harness = InMemoryWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_watermarks_are_per_subscription_and_event_type,
    harness = InMemoryWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_remove_watermark_of_one_event_type,
    harness = InMemoryWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_webhooks_repo_tests::webhook_watermark_repository_test_suite::test_remove_watermarks_of_subscription,
    harness = InMemoryWebhookWatermarkRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryWebhookWatermarkRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryWebhookWatermarkRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryWebhookSubscriptionEventStore>();
        catalog_builder.add::<InMemoryWebhookWatermarkRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
