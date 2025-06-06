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
use kamu_task_system as ts;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookDeliveryRepository {
    state: Arc<RwLock<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    webhook_deliveries: HashMap<ts::TaskID, WebhookDelivery>,
    webhooks_by_event_id: HashMap<WebhookEventID, Vec<ts::TaskID>>,
    webhooks_by_subscription_id: HashMap<WebhookSubscriptionID, Vec<ts::TaskID>>,
}

impl State {
    fn new() -> Self {
        Self::default()
    }

    fn list_deliveries_by_attempt_ids(&self, ids: &[ts::TaskID]) -> Vec<WebhookDelivery> {
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
        let task_id = delivery.task_id;
        let event_id = delivery.webhook_event_id;
        let subscription_id = delivery.webhook_subscription_id;

        let mut state = self.state.write().unwrap();

        if state.webhook_deliveries.insert(task_id, delivery).is_some() {
            return Err(CreateWebhookDeliveryError::DeliveryExists(
                WebhookDeliveryAlreadyExistsError { task_id },
            ));
        }

        state
            .webhooks_by_event_id
            .entry(event_id)
            .or_default()
            .push(task_id);

        state
            .webhooks_by_subscription_id
            .entry(subscription_id)
            .or_default()
            .push(task_id);

        Ok(())
    }

    async fn update_response(
        &self,
        task_id: ts::TaskID,
        response: WebhookResponse,
    ) -> Result<(), UpdateWebhookDeliveryError> {
        let mut state = self.state.write().unwrap();
        let delivery = state.webhook_deliveries.get_mut(&task_id).ok_or(
            UpdateWebhookDeliveryError::NotFound(WebhookDeliveryNotFoundError { task_id }),
        )?;
        delivery.set_response(response);

        Ok(())
    }

    async fn get_by_task_id(
        &self,
        task_id: ts::TaskID,
    ) -> Result<Option<WebhookDelivery>, GetWebhookDeliveryError> {
        let state = self.state.read().unwrap();
        Ok(state.webhook_deliveries.get(&task_id).cloned())
    }

    async fn list_by_event_id(
        &self,
        event_id: WebhookEventID,
    ) -> Result<Vec<WebhookDelivery>, ListWebhookDeliveriesError> {
        let state = self.state.read().unwrap();
        if let Some(task_attempt_ids) = state.webhooks_by_event_id.get(&event_id) {
            Ok(state.list_deliveries_by_attempt_ids(task_attempt_ids))
        } else {
            Ok(vec![])
        }
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
