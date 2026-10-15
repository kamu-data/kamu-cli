// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;
use kamu_webhooks::*;
use kamu_webhooks_inmem::{
    InMemoryWebhookSubscriptionEventStore,
    InMemoryWebhookWatermarkRepository,
};
use kamu_webhooks_services::{WebhookSubscriptionQueryServiceImpl, WebhookWatermarkInitializer};
use messaging_outbox::MockOutbox;
use secrecy::SecretString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct WebhookSubscriptionUseCaseHarness {
    catalog: Catalog,
    event_store: Arc<dyn WebhookSubscriptionEventStore>,
    query_service: Arc<dyn WebhookSubscriptionQueryService>,
    watermark_repo: Arc<dyn WebhookWatermarkRepository>,
}

impl WebhookSubscriptionUseCaseHarness {
    pub(crate) fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemoryWebhookSubscriptionEventStore>();
        b.add::<WebhookSubscriptionQueryServiceImpl>();
        b.add::<InMemoryWebhookWatermarkRepository>();
        b.add::<WebhookWatermarkInitializer>();
        b.add::<FakeDatasetRefUpdatedWatermarkProvider>();

        let catalog = b.build();

        Self {
            event_store: catalog.get_one().unwrap(),
            query_service: catalog.get_one().unwrap(),
            watermark_repo: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub(crate) async fn create_subscription_in_dataset(
        &self,
        dataset_id: odf::DatasetID,
    ) -> WebhookSubscription {
        self.create_subscription_in_dataset_with_label(dataset_id, None)
            .await
    }

    pub(crate) async fn create_subscription_in_dataset_with_label(
        &self,
        dataset_id: odf::DatasetID,
        label: Option<WebhookSubscriptionLabel>,
    ) -> WebhookSubscription {
        self.create_subscription(dataset_id, label, vec![WebhookEventTypeCatalog::test()])
            .await
    }

    pub(crate) async fn create_subscription_in_dataset_with_event_types(
        &self,
        dataset_id: odf::DatasetID,
        event_types: Vec<WebhookEventType>,
    ) -> WebhookSubscription {
        self.create_subscription(dataset_id, None, event_types)
            .await
    }

    async fn create_subscription(
        &self,
        dataset_id: odf::DatasetID,
        label: Option<WebhookSubscriptionLabel>,
        event_types: Vec<WebhookEventType>,
    ) -> WebhookSubscription {
        let mut subscription = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            label.unwrap_or_else(|| WebhookSubscriptionLabel::try_new("test_label").unwrap()),
            Some(dataset_id),
            event_types,
            WebhookSubscriptionSecret::try_new(None, &SecretString::from("secret")).unwrap(),
        );

        subscription.save(self.event_store.as_ref()).await.unwrap();
        subscription
    }

    pub(crate) async fn find_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
        query_mode: WebhookSubscriptionQueryMode,
    ) -> Option<WebhookSubscription> {
        self.query_service
            .find_webhook_subscription(subscription_id, query_mode)
            .await
            .unwrap()
    }

    pub(crate) async fn find_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Option<WebhookWatermark> {
        self.watermark_repo
            .find_watermark(subscription_id, event_type)
            .await
            .unwrap()
    }

    pub(crate) async fn set_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        watermark: WebhookWatermark,
    ) {
        self.watermark_repo
            .set_watermark(subscription_id, event_type, watermark)
            .await
            .unwrap();
    }

    /// Watermark the fake provider starts dataset update events of the dataset
    /// from
    pub(crate) fn initial_watermark(dataset_id: &odf::DatasetID) -> WebhookWatermark {
        FakeDatasetRefUpdatedWatermarkProvider::watermark(dataset_id)
    }

    pub(crate) fn stale_watermark() -> WebhookWatermark {
        WebhookWatermark::from(serde_json::json!({ "stale": true }))
    }

    pub(crate) fn expect_webhook_event_enabled_message(
        mock_outbox: &mut MockOutbox,
        event_type: &WebhookEventType,
        dataset_id: Option<&odf::DatasetID>,
        times: usize,
    ) {
        use mockall::predicate::{eq, function};

        let dataset_id = dataset_id.cloned();
        let event_type = event_type.clone();

        mock_outbox
            .expect_post_message_as_json()
            .times(times)
            .with(
                eq(MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE),
                function(move |message_as_json: &serde_json::Value| {
                    let message: WebhookSubscriptionEventChangesMessage =
                        serde_json::from_value(message_as_json.clone()).unwrap();

                    if let WebhookSubscriptionEventChangesMessage::EventEnabled(e) = &message {
                        e.dataset_id == dataset_id && e.event_type == event_type
                    } else {
                        false
                    }
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }

    pub(crate) fn expect_webhook_event_disabled_message(
        mock_outbox: &mut MockOutbox,
        event_type: &WebhookEventType,
        dataset_id: Option<&odf::DatasetID>,
        times: usize,
    ) {
        use mockall::predicate::{eq, function};

        let dataset_id = dataset_id.cloned();
        let event_type = event_type.clone();

        mock_outbox
            .expect_post_message_as_json()
            .times(times)
            .with(
                eq(MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_EVENT_CHANGES_SERVICE),
                function(move |message_as_json: &serde_json::Value| {
                    let message: WebhookSubscriptionEventChangesMessage =
                        serde_json::from_value(message_as_json.clone()).unwrap();

                    if let WebhookSubscriptionEventChangesMessage::EventDisabled(e) = &message {
                        e.dataset_id == dataset_id && e.event_type == event_type
                    } else {
                        false
                    }
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }

    pub(crate) fn expect_webhook_subscription_deleted_message(
        mock_outbox: &mut MockOutbox,
        event_type: &WebhookEventType,
        dataset_id: Option<&odf::DatasetID>,
        times: usize,
    ) {
        use mockall::predicate::{eq, function};

        let dataset_id = dataset_id.cloned();
        let event_types = vec![event_type.clone()];

        mock_outbox
            .expect_post_message_as_json()
            .times(times)
            .with(
                eq(MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE),
                function(move |message_as_json: &serde_json::Value| {
                    let message: WebhookSubscriptionLifecycleMessage =
                        serde_json::from_value(message_as_json.clone()).unwrap();

                    if let WebhookSubscriptionLifecycleMessage::Deleted(e) = &message {
                        e.dataset_id == dataset_id && e.event_types == event_types
                    } else {
                        false
                    }
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }

    pub(crate) fn expect_webhook_subscription_marked_unreachable_message(
        mock_outbox: &mut MockOutbox,
        event_type: &WebhookEventType,
        dataset_id: Option<&odf::DatasetID>,
        times: usize,
    ) {
        use mockall::predicate::{eq, function};

        let dataset_id = dataset_id.cloned();
        let event_types = vec![event_type.clone()];

        mock_outbox
            .expect_post_message_as_json()
            .times(times)
            .with(
                eq(MESSAGE_PRODUCER_KAMU_WEBHOOK_SUBSCRIPTION_SERVICE),
                function(move |message_as_json: &serde_json::Value| {
                    let message: WebhookSubscriptionLifecycleMessage =
                        serde_json::from_value(message_as_json.clone()).unwrap();

                    if let WebhookSubscriptionLifecycleMessage::MarkedUnreachable(e) = &message {
                        e.dataset_id == dataset_id && e.event_types == event_types
                    } else {
                        false
                    }
                }),
                eq(1),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Starts dataset update events of a subscription from a position derived
/// from its dataset
#[dill::component]
#[dill::interface(dyn WebhookEventWatermarkProvider)]
struct FakeDatasetRefUpdatedWatermarkProvider {}

impl FakeDatasetRefUpdatedWatermarkProvider {
    fn watermark(dataset_id: &odf::DatasetID) -> WebhookWatermark {
        WebhookWatermark::from(serde_json::json!({ "dataset_id": dataset_id.to_string() }))
    }
}

#[async_trait::async_trait]
impl WebhookEventWatermarkProvider for FakeDatasetRefUpdatedWatermarkProvider {
    fn event_type(&self) -> WebhookEventType {
        WebhookEventTypeCatalog::dataset_ref_updated()
    }

    async fn initial_watermark(
        &self,
        subscription: &WebhookSubscription,
    ) -> Result<Option<WebhookWatermark>, InternalError> {
        Ok(subscription.dataset_id().map(Self::watermark))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
