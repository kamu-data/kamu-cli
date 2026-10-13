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
    indexes: Indexes,
}

#[derive(Default, Clone)]
struct Indexes {
    webhook_subscriptions_by_dataset: HashMap<odf::DatasetID, Vec<WebhookSubscriptionID>>,
    webhook_subscription_data: HashMap<WebhookSubscriptionID, WebhookSubscriptionState>,
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

    /// Rejects events that would break the indexes, such as a duplicate label,
    /// without changing them
    fn check_index_updates(
        &self,
        events: &[WebhookSubscriptionEvent],
    ) -> Result<(), InternalError> {
        let state = self.inner.as_state();
        let mut indexes = state.lock().unwrap().indexes.clone();
        for event in events {
            Self::update_index(&mut indexes, event)?;
        }
        Ok(())
    }

    /// Indexes are changed only once the save passed the concurrent
    /// modification check, as a rejected save must leave them intact
    fn apply_index_updates(
        &self,
        events: &[WebhookSubscriptionEvent],
    ) -> Result<(), InternalError> {
        let state = self.inner.as_state();
        let mut g = state.lock().unwrap();
        for event in events {
            Self::update_index(&mut g.indexes, event)?;
        }
        Ok(())
    }

    fn update_index(
        indexes: &mut Indexes,
        event: &WebhookSubscriptionEvent,
    ) -> Result<(), InternalError> {
        match event {
            WebhookSubscriptionEvent::Created(e) => {
                if let Some(dataset_id) = &e.dataset_id {
                    Self::check_unique_label_within_dataset(indexes, dataset_id, &e.label)?;

                    indexes
                        .webhook_subscriptions_by_dataset
                        .entry(dataset_id.clone())
                        .or_default()
                        .push(e.subscription_id);
                }

                let subscription_state =
                    WebhookSubscriptionState::apply(None, event.clone()).unwrap();

                indexes
                    .webhook_subscription_data
                    .insert(e.subscription_id, subscription_state);
            }

            WebhookSubscriptionEvent::Modified(e) => {
                if let Some(subscription) = indexes
                    .webhook_subscription_data
                    .get(event.subscription_id())
                    && let Some(dataset_id) = subscription.dataset_id()
                    && subscription.label() != &e.new_label
                {
                    // Check if the new label is unique for the dataset
                    Self::check_unique_label_within_dataset(indexes, dataset_id, &e.new_label)?;
                }
                Self::update_subscription_state(indexes, event);
            }

            WebhookSubscriptionEvent::Enabled(_)
            | WebhookSubscriptionEvent::Paused(_)
            | WebhookSubscriptionEvent::Resumed(_)
            | WebhookSubscriptionEvent::MarkedUnreachable(_)
            | WebhookSubscriptionEvent::Reactivated(_)
            | WebhookSubscriptionEvent::SecretRotated(_)
            | WebhookSubscriptionEvent::Removed(_) => {
                Self::update_subscription_state(indexes, event);
            }
        }

        Ok(())
    }

    fn check_unique_label_within_dataset(
        indexes: &Indexes,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<(), InternalError> {
        if label.as_ref().is_empty() {
            return Ok(());
        }

        if let Some(ids) = indexes.webhook_subscriptions_by_dataset.get(dataset_id)
            && ids.iter().any(|id| {
                indexes
                    .webhook_subscription_data
                    .get(id)
                    .map(|subscription| subscription.label() == label)
                    .unwrap_or(false)
            })
        {
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

        Ok(())
    }

    fn update_subscription_state(indexes: &mut Indexes, event: &WebhookSubscriptionEvent) {
        if let Some(subscription_state) = indexes
            .webhook_subscription_data
            .get_mut(event.subscription_id())
        {
            *subscription_state =
                WebhookSubscriptionState::apply(Some(subscription_state.clone()), event.clone())
                    .unwrap();

            if subscription_state.status() == WebhookSubscriptionStatus::Removed
                && let Some(dataset_id) = subscription_state.dataset_id()
                && let Some(ids) = indexes.webhook_subscriptions_by_dataset.get_mut(dataset_id)
            {
                ids.retain(|id| id != event.subscription_id());
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
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        self.inner.total_events_stored().await
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, WebhookSubscriptionEvent> {
        self.inner.get_all_events(opts)
    }

    fn get_events(
        &self,
        subscription_id: &WebhookSubscriptionID,
        opts: GetEventsOpts,
    ) -> EventStream<'_, WebhookSubscriptionEvent> {
        self.inner.get_events(subscription_id, opts)
    }

    fn get_events_multi(
        &self,
        queries: &[WebhookSubscriptionID],
    ) -> MultiEventStream<'_, WebhookSubscriptionID, WebhookSubscriptionEvent> {
        self.inner.get_events_multi(queries)
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

        self.check_index_updates(&events)?;

        let last_event_id = self
            .inner
            .save_events(subscription_id, maybe_prev_stored_event_id, events.clone())
            .await?;

        self.apply_index_updates(&events)?;

        Ok(last_event_id)
    }

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<WebhookSubscriptionID, WebhookSubscriptionEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        validate_multi_save_items(&items)?;

        let events: Vec<_> = items
            .iter()
            .flat_map(|item| item.events.iter().cloned())
            .collect();

        self.check_index_updates(&events)?;

        let last_event_ids = self.inner.save_events_multi(items).await?;

        self.apply_index_updates(&events)?;

        Ok(last_event_ids)
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
        Ok(g.indexes
            .webhook_subscriptions_by_dataset
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
        Ok(g.indexes
            .webhook_subscriptions_by_dataset
            .get(dataset_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn list_all_subscription_ids(
        &self,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        Ok(g.indexes
            .webhook_subscription_data
            .iter()
            .filter_map(|(id, data)| {
                (data.status() != WebhookSubscriptionStatus::Removed).then_some(*id)
            })
            .collect())
    }

    async fn find_subscription_id_by_dataset_and_label(
        &self,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<Option<WebhookSubscriptionID>, FindWebhookSubscriptionError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        let maybe_subscription_id = g
            .indexes
            .webhook_subscriptions_by_dataset
            .get(dataset_id)
            .and_then(|ids| {
                ids.iter()
                    .find(|id| {
                        g.indexes
                            .webhook_subscription_data
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
        let maybe_subscription_ids = g
            .indexes
            .webhook_subscriptions_by_dataset
            .get(dataset_id)
            .and_then(|ids| {
                ids.iter()
                    .filter(|id| {
                        g.indexes
                            .webhook_subscription_data
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
