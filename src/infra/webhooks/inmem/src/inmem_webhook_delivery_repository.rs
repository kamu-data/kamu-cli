// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use database_common::PaginationOpts;
use dill::*;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookDeliveryRepository {
    state: Arc<RwLock<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    webhook_deliveries: HashMap<WebhookDeliveryID, WebhookDelivery>,
    webhooks_by_subscription_id: HashMap<WebhookSubscriptionID, Vec<WebhookDeliveryID>>,
}

impl State {
    fn new() -> Self {
        Self::default()
    }

    fn list_deliveries_by_attempt_ids(&self, ids: &[WebhookDeliveryID]) -> Vec<WebhookDelivery> {
        ids.iter()
            .filter_map(|id| self.webhook_deliveries.get(id).cloned())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookDeliveryRepository)]
#[scope(Singleton)]
impl InMemoryWebhookDeliveryRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookDeliveryRepository for InMemoryWebhookDeliveryRepository {
    async fn create(&self, delivery: WebhookDelivery) -> Result<(), CreateWebhookDeliveryError> {
        let webhook_delivery_id = delivery.webhook_delivery_id;
        let subscription_id = delivery.webhook_subscription_id;

        let mut state = self.state.write().unwrap();

        if state
            .webhook_deliveries
            .insert(webhook_delivery_id, delivery)
            .is_some()
        {
            return Err(CreateWebhookDeliveryError::DeliveryExists(
                WebhookDeliveryAlreadyExistsError {
                    webhook_delivery_id,
                },
            ));
        }

        state
            .webhooks_by_subscription_id
            .entry(subscription_id)
            .or_default()
            .push(webhook_delivery_id);

        Ok(())
    }

    async fn update_response(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut state = self.state.write().unwrap();
        let delivery = state
            .webhook_deliveries
            .get_mut(&webhook_delivery_id)
            .ok_or(UpdateWebhookDeliveryError::NotFound(
                WebhookDeliveryNotFoundError {
                    webhook_delivery_id,
                },
            ))?;
        delivery.set_response(response);

        Ok(())
    }

    async fn get_by_webhook_delivery_id(
        &self,
        webhook_delivery_id: WebhookDeliveryID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let state = self.state.read().unwrap();
        Ok(state.webhook_deliveries.get(&webhook_delivery_id).cloned())
    }

    async fn list_by_subscription_id(
        &self,
        subscription_id: WebhookSubscriptionID,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let state = self.state.read().unwrap();
        if let Some(all_task_attempt_ids) = state.webhooks_by_subscription_id.get(&subscription_id)
        {
            let task_attempt_ids = all_task_attempt_ids
                .iter()
                .rev()
                .skip(pagination.offset)
                .take(pagination.limit)
                .copied()
                .collect::<Vec<_>>();
            Ok(state.list_deliveries_by_attempt_ids(&task_attempt_ids))
        } else {
            Ok(vec![])
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
