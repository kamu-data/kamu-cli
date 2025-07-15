// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::InternalError;
use kamu_datasets::DatasetEntryService;
use kamu_webhooks::{ResultIntoInternal, WebhookPayloadBuilder};
use serde_json::json;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WEBHOOK_DATASET_REF_UPDATED_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookPayloadBuilder)]
pub struct WebhookPayloadBuilderImpl {
    dataset_entry_svc: Arc<dyn DatasetEntryService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookPayloadBuilder for WebhookPayloadBuilderImpl {
    async fn build_dataset_ref_updated_payload(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        new_block_hash: &odf::Multihash,
        maybe_prev_block_hash: Option<&odf::Multihash>,
    ) -> Result<serde_json::Value, InternalError> {
        // Find out who is the owner of the dataset
        let dataset_entry = self
            .dataset_entry_svc
            .get_entry(dataset_id)
            .await
            .int_err()?;

        // Form event payload
        let mut payload = json!({
            "version": WEBHOOK_DATASET_REF_UPDATED_VERSION,
            "datasetId": dataset_id.to_string(),
            "ownerAccountId": dataset_entry.owner_id.to_string(),
            "blockRef": block_ref.to_string(),
            "newHash": new_block_hash.to_string(),
        });

        // Add optional fields
        if let Some(prev_block_hash) = maybe_prev_block_hash.as_ref() {
            if let serde_json::Value::Object(ref mut map) = payload {
                map.insert("oldHash".to_string(), json!(prev_block_hash.to_string()));
            } else {
                unreachable!("Expected a JSON object");
            }
        }

        tracing::debug!(?payload, "Formed payload for dataset head updated event");

        Ok(payload)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
