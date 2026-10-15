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
use kamu_adapter_flow_webhook::{DatasetRefUpdatedWatermark, DatasetRefUpdatedWatermarkProvider};
use kamu_webhooks::{WebhookEventTypeCatalog, WebhookSubscriptionID, WebhookWatermarkRepository};
use kamu_webhooks_inmem::InMemoryWebhookWatermarkRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Watermarks of dataset update subscriptions, kept in memory and read
/// through `DatasetRefUpdatedWatermarkProvider`
pub(crate) struct DatasetWebhookWatermarks {
    watermark_repo: Arc<dyn WebhookWatermarkRepository>,
}

impl DatasetWebhookWatermarks {
    /// Registers the provider with the in-memory repository it reads; the
    /// dataset reference service it also needs is up to the caller
    pub(crate) fn register(b: &mut CatalogBuilder) {
        b.add::<DatasetRefUpdatedWatermarkProvider>()
            .add::<InMemoryWebhookWatermarkRepository>();
    }

    pub(crate) fn from_catalog(catalog: &Catalog) -> Self {
        Self {
            watermark_repo: catalog.get_one().unwrap(),
        }
    }

    /// Records that the receiver of the subscription knows `head`
    pub(crate) async fn set_delivered_head(
        &self,
        subscription_id: WebhookSubscriptionID,
        head: &odf::Multihash,
    ) {
        self.watermark_repo
            .set_watermark(
                subscription_id,
                &WebhookEventTypeCatalog::dataset_ref_updated(),
                DatasetRefUpdatedWatermark { head: head.clone() }.into(),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
