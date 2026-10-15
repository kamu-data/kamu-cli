// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use dill::Catalog;
use kamu_datasets::{DatasetEntry, DatasetEntryRepository, DatasetReferenceRepository};
use kamu_webhooks::*;
use secrecy::SecretString;
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Subscriptions and deliveries stored before the watermarks backfill, with
/// the watermark the backfill must start each subscription from
pub struct WebhookWatermarksBackfillScenario {
    pub expected_watermarks: Vec<(WebhookSubscriptionID, Option<WebhookWatermark>)>,
}

impl WebhookWatermarksBackfillScenario {
    pub async fn write(catalog: &Catalog) -> Self {
        let writer = ScenarioWriter::new(catalog);

        let foo_head = odf::Multihash::from_digest_sha3_256(b"foo-head");
        let bar_head = odf::Multihash::from_digest_sha3_256(b"bar-head");
        let foo_id = writer.create_dataset("foo", Some(&foo_head)).await;
        let bar_id = writer.create_dataset("bar", Some(&bar_head)).await;
        let baz_id = writer.create_dataset("baz", None).await;

        let delivered_head = odf::Multihash::from_digest_sha3_256(b"delivered");
        let delivered_v1_head = odf::Multihash::from_digest_sha3_256(b"delivered-v1");

        // The newest successful delivery counts, not a newer failed one
        let delivered = writer
            .create_subscription(&foo_id, WebhookEventTypeCatalog::dataset_ref_updated())
            .await;
        writer
            .deliver(delivered, 1, json!({ "new_hash": "older" }), 200)
            .await;
        writer
            .deliver(
                delivered,
                2,
                json!({ "new_hash": delivered_head.to_string() }),
                204,
            )
            .await;
        writer
            .deliver(delivered, 3, json!({ "new_hash": "failed" }), 500)
            .await;

        // Version 1 payloads name the head differently
        let delivered_v1 = writer
            .create_subscription(&foo_id, WebhookEventTypeCatalog::dataset_ref_updated())
            .await;
        writer
            .deliver(
                delivered_v1,
                1,
                json!({ "newHash": delivered_v1_head.to_string() }),
                200,
            )
            .await;

        // Never acknowledged anything: starts from the current head
        let undelivered = writer
            .create_subscription(&bar_id, WebhookEventTypeCatalog::dataset_ref_updated())
            .await;
        writer
            .deliver(undelivered, 1, json!({ "new_hash": "failed" }), 500)
            .await;

        let without_head = writer
            .create_subscription(&baz_id, WebhookEventTypeCatalog::dataset_ref_updated())
            .await;

        let removed = writer
            .create_subscription(&foo_id, WebhookEventTypeCatalog::dataset_ref_updated())
            .await;
        writer
            .deliver(
                removed,
                1,
                json!({ "new_hash": delivered_head.to_string() }),
                200,
            )
            .await;
        writer.remove_subscription(removed).await;

        let other_event_type = writer
            .create_subscription(&foo_id, WebhookEventTypeCatalog::test())
            .await;

        Self {
            expected_watermarks: vec![
                (delivered, Some(head_watermark(&delivered_head))),
                (delivered_v1, Some(head_watermark(&delivered_v1_head))),
                (undelivered, Some(head_watermark(&bar_head))),
                (without_head, None),
                (removed, None),
                (other_event_type, None),
            ],
        }
    }
}

fn head_watermark(head: &odf::Multihash) -> WebhookWatermark {
    WebhookWatermark::from(json!({ "head": head.to_string() }))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ScenarioWriter<'a> {
    catalog: &'a Catalog,
    start_time: DateTime<Utc>,
}

impl<'a> ScenarioWriter<'a> {
    fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            start_time: Utc::now().round_subsecs(6),
        }
    }

    async fn create_dataset(&self, name: &str, head: Option<&odf::Multihash>) -> odf::DatasetID {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(name.as_bytes());

        self.catalog
            .get_one::<dyn DatasetEntryRepository>()
            .unwrap()
            .save_dataset_entry(&DatasetEntry::new(
                dataset_id.clone(),
                odf::AccountID::new_seeded_ed25519(b"owner"),
                odf::AccountName::new_unchecked("owner"),
                odf::DatasetName::new_unchecked(name),
                self.start_time,
                odf::DatasetKind::Root,
            ))
            .await
            .unwrap();

        if let Some(head) = head {
            self.catalog
                .get_one::<dyn DatasetReferenceRepository>()
                .unwrap()
                .set_dataset_reference(&dataset_id, &odf::BlockRef::Head, None, head)
                .await
                .unwrap();
        }

        dataset_id
    }

    async fn create_subscription(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: WebhookEventType,
    ) -> WebhookSubscriptionID {
        let subscription_id = WebhookSubscriptionID::new(uuid::Uuid::new_v4());
        let mut subscription = WebhookSubscription::new(
            subscription_id,
            url::Url::parse("https://example.com").unwrap(),
            WebhookSubscriptionLabel::try_new(subscription_id.to_string()).unwrap(),
            Some(dataset_id.clone()),
            vec![event_type],
            WebhookSubscriptionSecret::try_new(None, &SecretString::from("some-secret")).unwrap(),
        );
        subscription.enable().unwrap();
        subscription
            .save(self.event_store().as_ref())
            .await
            .unwrap();

        subscription_id
    }

    async fn remove_subscription(&self, subscription_id: WebhookSubscriptionID) {
        let event_store = self.event_store();
        let mut subscription = WebhookSubscription::load(&subscription_id, event_store.as_ref())
            .await
            .unwrap();
        subscription.remove().unwrap();
        subscription.save(event_store.as_ref()).await.unwrap();
    }

    /// A delivery of the dataset update event, sent `minute` minutes after the
    /// start and answered with `status_code`
    async fn deliver(
        &self,
        subscription_id: WebhookSubscriptionID,
        minute: i64,
        payload: serde_json::Value,
        status_code: u16,
    ) {
        let delivery_repo = self
            .catalog
            .get_one::<dyn WebhookDeliveryRepository>()
            .unwrap();

        let delivery_id = WebhookDeliveryID::new(uuid::Uuid::new_v4());
        let requested_at = self.start_time + Duration::minutes(minute);
        delivery_repo
            .create(WebhookDelivery::new(
                delivery_id,
                subscription_id,
                WebhookEventTypeCatalog::dataset_ref_updated(),
                WebhookRequest {
                    headers: http::HeaderMap::new(),
                    started_at: requested_at,
                    payload,
                },
            ))
            .await
            .unwrap();
        delivery_repo
            .update_response(
                delivery_id,
                WebhookResponse {
                    status_code: http::StatusCode::from_u16(status_code).unwrap(),
                    headers: http::HeaderMap::new(),
                    body: String::new(),
                    finished_at: requested_at + Duration::seconds(1),
                },
            )
            .await
            .unwrap();
    }

    fn event_store(&self) -> Arc<dyn WebhookSubscriptionEventStore> {
        self.catalog
            .get_one::<dyn WebhookSubscriptionEventStore>()
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
