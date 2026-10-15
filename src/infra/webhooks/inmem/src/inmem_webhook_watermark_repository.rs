// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use dill::*;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookWatermarkRepository {
    state: Arc<RwLock<State>>,
}

#[derive(Default)]
struct State {
    watermarks: HashMap<WebhookSubscriptionID, BTreeMap<WebhookEventType, WebhookWatermark>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookWatermarkRepository)]
#[scope(Singleton)]
impl InMemoryWebhookWatermarkRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookWatermarkRepository for InMemoryWebhookWatermarkRepository {
    async fn find_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<Option<WebhookWatermark>, InternalError> {
        let state = self.state.read().unwrap();
        Ok(state
            .watermarks
            .get(&subscription_id)
            .and_then(|watermarks| watermarks.get(event_type))
            .cloned())
    }

    async fn set_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        watermark: WebhookWatermark,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().unwrap();
        state
            .watermarks
            .entry(subscription_id)
            .or_default()
            .insert(event_type.clone(), watermark);
        Ok(())
    }

    async fn remove_watermark(
        &self,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().unwrap();
        if let Some(watermarks) = state.watermarks.get_mut(&subscription_id) {
            watermarks.remove(event_type);
        }
        Ok(())
    }

    async fn remove_watermarks(
        &self,
        subscription_id: WebhookSubscriptionID,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().unwrap();
        state.watermarks.remove(&subscription_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
