// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use dill::{Catalog, CatalogBuilder};
use kamu_datasets_postgres::{PostgresDatasetEntryRepository, PostgresDatasetReferenceRepository};
use kamu_webhooks_postgres::*;
use kamu_webhooks_repo_tests::WebhookWatermarksBackfillScenario;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BACKFILL_MIGRATION: &str = include_str!(
    "../../../../../../migrations/postgres/20261015072339_webhook_watermarks_backfill.sql"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_backfill_starts_watermarks_of_existing_subscriptions(pg_pool: PgPool) {
    let harness = WebhookWatermarksBackfillHarness::new(pg_pool);

    let scenario = WebhookWatermarksBackfillScenario::write_committed(&harness.catalog).await;

    harness.run_backfill().await;

    scenario.assert_watermarks(&harness.catalog).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookWatermarksBackfillHarness {
    pg_pool: PgPool,
    catalog: Catalog,
}

impl WebhookWatermarksBackfillHarness {
    fn new(pg_pool: PgPool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(pg_pool.clone());
        b.add::<PostgresTransactionManager>();
        b.add::<PostgresDatasetEntryRepository>();
        b.add::<PostgresDatasetReferenceRepository>();
        b.add::<PostgresWebhookSubscriptionEventStore>();
        b.add::<PostgresWebhookDeliveryRepository>();
        b.add::<PostgresWebhookWatermarkRepository>();

        Self {
            pg_pool,
            catalog: b.build(),
        }
    }

    async fn run_backfill(&self) {
        sqlx::raw_sql(BACKFILL_MIGRATION)
            .execute(&self.pg_pool)
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
