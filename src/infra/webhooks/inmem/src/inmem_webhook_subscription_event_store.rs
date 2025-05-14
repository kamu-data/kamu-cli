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
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookSubscriptionEventStore {
    inner: InMemoryEventStore<WebhookSubscriptionState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<WebhookSubscriptionEvent>,
    webhook_subscriptions_by_dataset: HashMap<odf::DatasetID, Vec<WebhookSubscriptionID>>,
    webhook_subscription_data: HashMap<WebhookSubscriptionID, WebhookSubscription>,
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
impl InMemoryWebhookSubscriptionEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }

    fn update_index(
        state: &mut State,
        event: &WebhookSubscriptionEvent,
    ) -> Result<(), InternalError> {
        match event {
            WebhookSubscriptionEvent::Created(e) => {
                if let Some(dataset_id) = &e.dataset_id {
                    Self::check_unique_label_within_dataset(state, dataset_id, &e.label)?;

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

            WebhookSubscriptionEvent::Modified(e) => {
                if let Some(subscription) =
                    state.webhook_subscription_data.get(event.subscription_id())
                {
                    if let Some(dataset_id) = subscription.dataset_id() {
                        Self::check_unique_label_within_dataset(state, dataset_id, &e.new_label)?;
                    }
                }
                Self::update_subscription_state(state, event);
            }

            _ => Self::update_subscription_state(state, event),
        }

        Ok(())
    }

    fn check_unique_label_within_dataset(
        state: &State,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<(), InternalError> {
        if let Some(ids) = state.webhook_subscriptions_by_dataset.get(dataset_id) {
            if ids.iter().any(|id| {
                state
                    .webhook_subscription_data
                    .get(id)
                    .map(|subscription| subscription.label() == label)
                    .unwrap_or(false)
            }) {
                #[derive(Error, Debug)]
                #[error(
                    "Webhook subscription label `{label}` is not unique for dataset `{dataset_id}`"
                )]
                struct NonUniqueLabelError {
                    label: WebhookSubscriptionLabel,
                    dataset_id: odf::DatasetID,
                }

                return Err(NonUniqueLabelError {
                    label: label.clone(),
                    dataset_id: dataset_id.clone(),
                }
                .int_err());
            }
        }

        Ok(())
    }

    fn update_subscription_state(state: &mut State, event: &WebhookSubscriptionEvent) {
        if let Some(subscription) = state
            .webhook_subscription_data
            .get_mut(event.subscription_id())
        {
            subscription.apply(event.clone()).unwrap();

            if subscription.status() == WebhookSubscriptionStatus::Removed {
                if let Some(dataset_id) = subscription.dataset_id() {
                    if let Some(ids) = state.webhook_subscriptions_by_dataset.get_mut(dataset_id) {
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<WebhookSubscriptionState> for InMemoryWebhookSubscriptionEventStore {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events(
        &self,
        subscription_id: &WebhookSubscriptionID,
        opts: GetEventsOpts,
    ) -> EventStream<WebhookSubscriptionEvent> {
        self.inner.get_events(subscription_id, opts)
    }

    async fn save_events(
        &self,
        subscription_id: &WebhookSubscriptionID,
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
                Self::update_index(&mut g, event)?;
            }
        }

        self.inner
            .save_events(subscription_id, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSubscriptionEventStore for InMemoryWebhookSubscriptionEventStore {
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
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
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
    ) -> Result<Option<WebhookSubscriptionID>, FindWebhookSubscriptionError> {
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

    async fn list_enabled_subscription_ids_by_dataset_and_event_type(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: &WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
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
                                .map(|data| {
                                    data.status() == WebhookSubscriptionStatus::Enabled
                                        && data.event_types().contains(event_type)
                                })
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
