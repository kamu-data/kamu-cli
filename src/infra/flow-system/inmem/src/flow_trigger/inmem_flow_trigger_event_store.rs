// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use dill::*;
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowTriggerEventStore {
    inner: InMemoryEventStore<FlowTriggerState, State>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowTriggerEvent>,
    dataset_ids: Vec<odf::DatasetID>,
}

impl EventStoreState<FlowTriggerState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowTriggerEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowTriggerEvent) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowTriggerEventStore)]
#[scope(Singleton)]
impl InMemoryFlowTriggerEventStore {
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowTriggerState> for InMemoryFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, ?opts))]
    fn get_events(
        &self,
        query: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<FlowTriggerEvent> {
        self.inner.get_events(query, opts)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, num_events = events.len()))]
    async fn save_events(
        &self,
        query: &FlowBinding,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowTriggerEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        if let FlowScope::Dataset { dataset_id } = &query.scope {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            g.dataset_ids.push(dataset_id.clone());
        }

        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerEventStore for InMemoryFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_active_flow_bindings(&self) -> FlowBindingStream {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_bindings = HashSet::new();
        let mut active_bindings = Vec::new();

        for event in g.events.iter().rev() {
            let flow_binding = event.flow_binding();

            // Only process the latest event per binding
            if !seen_bindings.insert(flow_binding.clone()) {
                continue;
            }

            let is_active = match event {
                FlowTriggerEvent::Created(e) => !e.paused,
                FlowTriggerEvent::Modified(e) => !e.paused,
                FlowTriggerEvent::ScopeRemoved { .. } => false,
            };

            if is_active {
                active_bindings.push(flow_binding.clone());
            }
        }

        // Convert into stream
        Box::pin(futures::stream::iter(active_bindings.into_iter().map(Ok)))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn all_trigger_bindings_for_dataset_flows(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_flow_types = HashSet::new();
        let mut bindings = Vec::new();

        for event in g.events.iter().rev() {
            if let FlowBinding {
                scope: FlowScope::Dataset { dataset_id: id },
                flow_type,
            } = event.flow_binding()
                && id == dataset_id
                && seen_flow_types.insert(flow_type)
            {
                bindings.push(FlowBinding::for_dataset(id.clone(), flow_type));
            }
        }

        Ok(bindings)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%webhook_subscription_id))]
    async fn all_trigger_bindings_for_webhook_flows(
        &self,
        webhook_subscription_id: uuid::Uuid,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_flow_types = HashSet::new();
        let mut bindings = Vec::new();

        for event in g.events.iter().rev() {
            if let FlowBinding {
                scope:
                    FlowScope::WebhookSubscription {
                        subscription_id,
                        dataset_id,
                    },
                flow_type,
            } = event.flow_binding()
                && *subscription_id == webhook_subscription_id
                && seen_flow_types.insert(flow_type)
            {
                bindings.push(FlowBinding::for_webhook_subscription(
                    *subscription_id,
                    dataset_id.clone(),
                    flow_type,
                ));
            }
        }

        Ok(bindings)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn all_trigger_bindings_for_system_flows(
        &self,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_flow_types = HashSet::new();
        let mut bindings = Vec::new();

        for event in g.events.iter().rev() {
            if let FlowBinding {
                scope: FlowScope::System,
                flow_type,
            } = event.flow_binding()
                && seen_flow_types.insert(flow_type)
            {
                bindings.push(FlowBinding::for_system(flow_type));
            }
        }

        Ok(bindings)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn has_active_triggers_for_datasets(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<bool, InternalError> {
        if dataset_ids.is_empty() {
            return Ok(false);
        }

        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let dataset_ids: HashSet<&odf::DatasetID> = dataset_ids.iter().collect();
        let mut seen_bindings = HashSet::new();

        for event in g.events.iter().rev() {
            // Skip if we've already seen this binding (we only want the latest event)
            let flow_binding = event.flow_binding();
            if !seen_bindings.insert(flow_binding) {
                continue;
            }

            match &flow_binding.scope {
                FlowScope::Dataset { dataset_id } if dataset_ids.contains(dataset_id) => {
                    match event {
                        FlowTriggerEvent::Created(e) => {
                            if !e.paused {
                                return Ok(true);
                            }
                        }
                        FlowTriggerEvent::Modified(e) => {
                            if !e.paused {
                                return Ok(true);
                            }
                        }
                        FlowTriggerEvent::ScopeRemoved { .. } => {
                            // permanently stopped — not active
                        }
                    }
                }
                _ => {} // skip system flows
            }
        }

        Ok(false)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
