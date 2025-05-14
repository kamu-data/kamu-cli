// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::{component, interface};
use internal_error::InternalError;
use kamu_datasets::{DatasetEntryService, DatasetReferenceMessageUpdated};
use kamu_webhooks::{
    ResultIntoInternal,
    WebhookEvent,
    WebhookEventBuilder,
    WebhookEventId,
    WebhookEventRepository,
    WebhookEventTypeCatalog,
};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_DATASET_HEAD_UPDATED_VERSION: &str = "1";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookEventBuilder)]
pub struct WebhookEventBuilderImpl {
    dataset_entry_svc: Arc<dyn DatasetEntryService>,
    webhook_event_repo: Arc<dyn WebhookEventRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookEventBuilder for WebhookEventBuilderImpl {
    async fn build_dataset_head_updated(
        &self,
        event: &DatasetReferenceMessageUpdated,
    ) -> Result<WebhookEvent, InternalError> {
        // Find out who is the owner of the dataset
        let dataset_entry = self
            .dataset_entry_svc
            .get_entry(&event.dataset_id)
            .await
            .int_err()?;

        // Form event payload
        let mut payload = json!({
            "version": WEBHOOK_DATASET_HEAD_UPDATED_VERSION,
            "datasetId": event.dataset_id.to_string(),
            "ownerAccountId": dataset_entry.owner_id.to_string(),
            "newHash": event.new_block_hash.to_string(),
        });

        // Add optional fields
        if let Some(prev_block_hash) = event.maybe_prev_block_hash.as_ref() {
            if let serde_json::Value::Object(ref mut map) = payload {
                map.insert("oldHash".to_string(), json!(prev_block_hash.to_string()));
            } else {
                unreachable!("Expected a JSON object");
            }
        }

        tracing::debug!(?payload, "Formed payload for dataset head updated event");

        // Create and register a webhook event
        let event_id = WebhookEventId::new(uuid::Uuid::new_v4());
        let event = WebhookEvent::new(
            event_id,
            WebhookEventTypeCatalog::dataset_head_updated(),
            payload,
            Utc::now(),
        );
        self.webhook_event_repo
            .create_event(&event)
            .await
            .int_err()?;

        Ok(event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
