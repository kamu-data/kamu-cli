// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::Arc;

use dill::*;
use kamu_flow_system::*;

use crate::InMemoryFlowSystemEventBridge;
use crate::flow_event_data_helper::FlowEventDataHelper;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowConfigurationEventStore {
    inner: InMemoryEventStore<FlowConfigurationState, State>,
    flow_system_event_store: Arc<InMemoryFlowSystemEventBridge>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowConfigurationEvent>,
}

impl EventStoreState<FlowConfigurationState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowConfigurationEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowConfigurationEvent) {
        self.events.push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowConfigurationEventStore)]
#[scope(Singleton)]
impl InMemoryFlowConfigurationEventStore {
    pub fn new(flow_system_event_store: Arc<InMemoryFlowSystemEventBridge>) -> Self {
        Self {
            inner: InMemoryEventStore::new(),
            flow_system_event_store,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for InMemoryFlowConfigurationEventStore {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, FlowConfigurationEvent> {
        self.inner.get_all_events(opts)
    }

    fn get_events(
        &self,
        query: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<'_, FlowConfigurationEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &FlowBinding,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        // Skip empty saves
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        // Prepare data for FlowSystemEventStore - a merged stream
        let merge_event_data = FlowEventDataHelper::prepare_merge_event_data(
            &events,
            FlowConfigurationEvent::event_time,
        );

        // Save events to this store
        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await?;

        // Save merged events to FlowSystemEventStore
        let global_event_id = self.flow_system_event_store.save_events(
            FlowSystemEventSourceType::FlowConfiguration,
            &merge_event_data,
        );

        // Return the global event ID as the result of this operation,
        // ignore local event ID in the inner store
        Ok(global_event_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowConfigurationEventStore for InMemoryFlowConfigurationEventStore {
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream<'_> {
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

            active_bindings.push(flow_binding.clone());
        }

        // Convert into stream
        Box::pin(futures::stream::iter(active_bindings.into_iter().map(Ok)))
    }

    async fn all_bindings_for_scope(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_flow_types = HashSet::new();
        let mut bindings = Vec::new();

        for event in g.events.iter().rev() {
            let flow_binding = event.flow_binding();
            if flow_binding.scope != *flow_scope {
                continue;
            }
            if seen_flow_types.insert(flow_binding.flow_type.clone()) {
                bindings.push(flow_binding.clone());
            }
        }

        Ok(bindings)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
