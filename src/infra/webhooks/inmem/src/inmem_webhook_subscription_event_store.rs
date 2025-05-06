// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::HashMap;

use dill::*;
use event_sourcing::*;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookSubscriptionStore {
    inner: InMemoryEventStore<WebhookSubscriptionState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<WebhookSubscriptionEvent>,
    webhook_subscriptions_by_dataset: HashMap<odf::DatasetID, Vec<WebhookSubscriptionId>>,
    webhook_subscription_data: HashMap<WebhookSubscriptionId, WebhookSubscription>,
}

impl EventStoreState<WebhookSubscriptionState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[<WebhookSubscriptionState as Projection>::Event] {
        &self.events
    }

    fn add_event(&mut self, event: <WebhookSubscriptionState as Projection>::Event) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookSubscriptionEventStore)]
#[scope(Singleton)]
impl InMemoryWebhookSubscriptionStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }

    fn update_index(state: &mut State, event: &WebhookSubscriptionEvent) {
        match event {
            WebhookSubscriptionEvent::Created(e) => {
                if let Some(dataset_id) = &e.dataset_id {
                    state
                        .webhook_subscriptions_by_dataset
                        .entry(dataset_id.clone())
                        .or_default()
                        .push(e.subscription_id);
                }

                let subscription = WebhookSubscription::new(
                    e.subscription_id,
                    e.target_url.clone(),
                    e.label.clone(),
                    e.dataset_id.clone(),
                    e.event_types.clone(),
                    e.secret.clone(),
                );

                state
                    .webhook_subscription_data
                    .insert(e.subscription_id, subscription);
            }

            _ => {
                if let Some(subscription) = state
                    .webhook_subscription_data
                    .get_mut(event.subscription_id())
                {
                    subscription.apply(event.clone()).unwrap();

                    if subscription.status() == WebhookSubscriptionStatus::Removed {
                        if let Some(dataset_id) = subscription.dataset_id() {
                            if let Some(ids) =
                                state.webhook_subscriptions_by_dataset.get_mut(dataset_id)
                            {
                                ids.retain(|id| id != event.subscription_id());
                            }
                        }
                    }
                } else {
                    panic!(
                        "WebhookSubscriptionEvent {} for unknown subscription {}",
                        event.typename(),
                        event.subscription_id()
                    );
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<WebhookSubscriptionState> for InMemoryWebhookSubscriptionStore {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events(
        &self,
        subscription_id: &WebhookSubscriptionId,
        opts: GetEventsOpts,
    ) -> EventStream<WebhookSubscriptionEvent> {
        self.inner.get_events(subscription_id, opts)
    }

    async fn save_events(
        &self,
        subscription_id: &WebhookSubscriptionId,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<WebhookSubscriptionEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index(&mut g, event);
            }
        }

        self.inner
            .save_events(subscription_id, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSubscriptionEventStore for InMemoryWebhookSubscriptionStore {
    async fn count_subscriptions_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, CountWebhookSubscriptionsError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.webhook_subscriptions_by_dataset
            .get(dataset_id)
            .map(Vec::len)
            .unwrap_or_default())
    }

    async fn list_subscription_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<WebhookSubscriptionId>, ListWebhookSubscriptionsError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.webhook_subscriptions_by_dataset
            .get(dataset_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn find_subscription_id_by_dataset_and_label(
        &self,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<Option<WebhookSubscriptionId>, FindWebhookSubscriptionError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        let maybe_subscription_id =
            g.webhook_subscriptions_by_dataset
                .get(dataset_id)
                .and_then(|ids| {
                    ids.iter()
                        .find(|id| {
                            g.webhook_subscription_data
                                .get(id)
                                .map(|subscription| subscription.label() == label)
                                .unwrap_or(false)
                        })
                        .copied()
                });
        Ok(maybe_subscription_id)
    }

    async fn list_subscription_ids_by_dataset_and_event_type(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionId>, ListWebhookSubscriptionsError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        let maybe_subscription_ids =
            g.webhook_subscriptions_by_dataset
                .get(dataset_id)
                .and_then(|ids| {
                    ids.iter()
                        .filter(|id| {
                            g.webhook_subscription_data
                                .get(id)
                                .map(|data| data.event_types().contains(&event_type))
                                .unwrap_or(false)
                        })
                        .copied()
                        .collect::<Vec<_>>()
                        .into()
                });
        Ok(maybe_subscription_ids.unwrap_or_default())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
