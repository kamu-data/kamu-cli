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

    pub(crate) async fn create_subscription(&self) -> WebhookSubscription {
        self.create_subscription_in_dataset(odf::DatasetID::new_seeded_ed25519(b"foo"), None)
            .await
    }

    pub(crate) async fn create_subscription_in_dataset(
        &self,
        dataset_id: odf::DatasetID,
        label: Option<WebhookSubscriptionLabel>,
    ) -> WebhookSubscription {
        let mut subscription = WebhookSubscription::new(
            WebhookSubscriptionID::new(uuid::Uuid::new_v4()),
            url::Url::parse("https://example.com/webhook").unwrap(),
            label.unwrap_or_else(|| WebhookSubscriptionLabel::new("test_label")),
            Some(dataset_id),
            vec![WebhookEventTypeCatalog::test()],
            WebhookSubscriptionSecret::try_new("secret").unwrap(),
        );

        subscription.save(self.event_store.as_ref()).await.unwrap();
        subscription
    }

    pub(crate) async fn find_subscription(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Option<WebhookSubscription> {
        self.query_service
            .find_webhook_subscription(subscription_id)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
