// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_adapter_flow_webhook::DatasetRefUpdatedWatermarkProvider;
use kamu_datasets::DatasetReferenceService;
use kamu_datasets_services::testing::FakeDatasetReferenceService;
use kamu_webhooks::*;
use secrecy::SecretString;
use serde_json::json;
use uuid::Uuid;

use super::utils::{DatasetWebhookWatermarks, LinearDatasetHistory};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_initial_watermark_is_current_head() {
    let head = LinearDatasetHistory::head(1);
    let harness = DatasetRefUpdatedWatermarkProviderHarness::new(Some(&head));

    // The same form the backfill migration writes
    pretty_assertions::assert_eq!(
        harness.initial_watermark().await,
        Some(WebhookWatermark::from(json!({ "head": head.to_string() })))
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_no_initial_watermark_without_head() {
    let harness = DatasetRefUpdatedWatermarkProviderHarness::new(None);

    pretty_assertions::assert_eq!(harness.initial_watermark().await, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delivered_head_read_from_watermark() {
    let head = LinearDatasetHistory::head(2);
    let harness = DatasetRefUpdatedWatermarkProviderHarness::new(None);

    pretty_assertions::assert_eq!(harness.find_delivered_head().await, None);

    harness.set_delivered_head(&head).await;

    pretty_assertions::assert_eq!(harness.find_delivered_head().await, Some(head));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetRefUpdatedWatermarkProviderHarness {
    provider: Arc<DatasetRefUpdatedWatermarkProvider>,
    watermarks: DatasetWebhookWatermarks,
}

impl DatasetRefUpdatedWatermarkProviderHarness {
    /// A subscription to "foo", whose head is `dataset_head`
    fn new(dataset_head: Option<&odf::Multihash>) -> Self {
        let dataset_references = FakeDatasetReferenceService::new();
        if let Some(dataset_head) = dataset_head {
            dataset_references.set_head(&Self::dataset_id(), dataset_head);
        }

        let mut b = dill::CatalogBuilder::new();
        b.add_value(dataset_references);
        b.bind::<dyn DatasetReferenceService, FakeDatasetReferenceService>();
        DatasetWebhookWatermarks::register(&mut b);
        let catalog = b.build();

        Self {
            provider: catalog.get_one().unwrap(),
            watermarks: DatasetWebhookWatermarks::from_catalog(&catalog),
        }
    }

    fn subscription_id() -> WebhookSubscriptionID {
        WebhookSubscriptionID::new(Uuid::from_u128(1))
    }

    fn dataset_id() -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(b"foo")
    }

    async fn initial_watermark(&self) -> Option<WebhookWatermark> {
        let subscription = WebhookSubscription::new(
            Self::subscription_id(),
            url::Url::parse("https://example.com/webhook").unwrap(),
            WebhookSubscriptionLabel::try_new("test").unwrap(),
            Some(Self::dataset_id()),
            vec![WebhookEventTypeCatalog::dataset_ref_updated()],
            WebhookSubscriptionSecret::try_new(None, &SecretString::from("secret")).unwrap(),
        );

        self.provider
            .initial_watermark(&subscription)
            .await
            .unwrap()
    }

    async fn set_delivered_head(&self, head: &odf::Multihash) {
        self.watermarks
            .set_delivered_head(Self::subscription_id(), head)
            .await;
    }

    async fn find_delivered_head(&self) -> Option<odf::Multihash> {
        self.provider
            .find_delivered_head(Self::subscription_id())
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
