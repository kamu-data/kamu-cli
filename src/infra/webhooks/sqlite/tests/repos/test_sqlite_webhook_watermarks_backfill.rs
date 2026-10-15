// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionRunner, SqliteTransactionManager};
use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_datasets_sqlite::{SqliteDatasetEntryRepository, SqliteDatasetReferenceRepository};
use kamu_webhooks::*;
use kamu_webhooks_repo_tests::WebhookWatermarksBackfillScenario;
use kamu_webhooks_sqlite::*;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BACKFILL_MIGRATION: &str = include_str!(
    "../../../../../../migrations/sqlite/20261015072339_webhook_watermarks_backfill.sql"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(sqlite)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/sqlite"))]
async fn test_backfill_starts_watermarks_of_existing_subscriptions(sqlite_pool: SqlitePool) {
    let harness = WebhookWatermarksBackfillHarness::new(sqlite_pool);

    let scenario = harness.write_scenario().await;

    harness.run_backfill().await;

    for (subscription_id, expected_watermark) in scenario.expected_watermarks {
        assert_eq!(
            harness.find_watermark(subscription_id).await,
            expected_watermark,
            "watermark of subscription {subscription_id}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct WebhookWatermarksBackfillHarness {
    sqlite_pool: SqlitePool,
    catalog: Catalog,
}

impl WebhookWatermarksBackfillHarness {
    fn new(sqlite_pool: SqlitePool) -> Self {
        let mut b = CatalogBuilder::new();
        b.add_value(sqlite_pool.clone());
        b.add::<SqliteTransactionManager>();
        b.add::<SqliteDatasetEntryRepository>();
        b.add::<SqliteDatasetReferenceRepository>();
        b.add::<SqliteWebhookSubscriptionEventStore>();
        b.add::<SqliteWebhookDeliveryRepository>();
        b.add::<SqliteWebhookWatermarkRepository>();

        Self {
            sqlite_pool,
            catalog: b.build(),
        }
    }

    fn transaction_runner(&self) -> DatabaseTransactionRunner {
        DatabaseTransactionRunner::new(self.catalog.clone())
    }

    async fn write_scenario(&self) -> WebhookWatermarksBackfillScenario {
        self.transaction_runner()
            .transactional(|catalog| async move {
                Ok::<_, InternalError>(WebhookWatermarksBackfillScenario::write(&catalog).await)
            })
            .await
            .unwrap()
    }

    async fn run_backfill(&self) {
        sqlx::raw_sql(BACKFILL_MIGRATION)
            .execute(&self.sqlite_pool)
            .await
            .unwrap();
    }

    async fn find_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Option<WebhookWatermark> {
        self.transaction_runner()
            .transactional_with(
                |watermark_repo: Arc<dyn WebhookWatermarkRepository>| async move {
                    watermark_repo
                        .find_watermark(
                            subscription_id,
                            &WebhookEventTypeCatalog::dataset_ref_updated(),
                        )
                        .await
                },
            )
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
