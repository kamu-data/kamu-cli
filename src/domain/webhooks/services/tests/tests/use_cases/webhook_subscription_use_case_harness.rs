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
use kamu_webhooks::*;
use kamu_webhooks_inmem::InMemoryWebhookSubscriptionEventStore;
use kamu_webhooks_services::WebhookSubscriptionQueryServiceImpl;
use messaging_outbox::MockOutbox;
use secrecy::SecretString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct WebhookSubscriptionUseCaseHarness {
    catalog: Catalog,
    event_store: Arc<dyn WebhookSubscriptionEventStore>,
    query_service: Arc<dyn WebhookSubscriptionQueryService>,
}

impl WebhookSubscriptionUseCaseHarness {
    pub(crate) fn new() -> Self {
        let mut b = CatalogBuilder::new();
        b.add::<InMemoryWebhookSubscriptionEventStore>();
        b.add::<WebhookSubscriptionQueryServiceImpl>();

        let catalog = b.build();

        Self {
            event_store: catalog.get_one().unwrap(),
            query_service: catalog.get_one().unwrap(),
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
        let mut subscription = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            label.unwrap_or_else(|| WebhookSubscriptionLabel::try_new("test_label").unwrap()),
            Some(dataset_id),
            vec![WebhookEventTypeCatalog::test()],
        );

        subscription
            .create_secret(
                WebhookSubscriptionSecret::try_new(None, &SecretString::from("secret")).unwrap(),
            )
            .unwrap();
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
