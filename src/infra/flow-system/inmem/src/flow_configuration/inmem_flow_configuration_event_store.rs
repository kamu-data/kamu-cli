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

pub struct InMemoryFlowConfigurationEventStore {
    inner: InMemoryEventStore<FlowConfigurationState, State>,
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
    pub fn new() -> Self {
        Self {
            inner: InMemoryEventStore::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowConfigurationState> for InMemoryFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, ?opts))]
    fn get_events(
        &self,
        query: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<FlowConfigurationEvent> {
        self.inner.get_events(query, opts)
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?query, num_events = events.len()))]
    async fn save_events(
        &self,
        query: &FlowBinding,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        self.inner
            .save_events(query, maybe_prev_stored_event_id, events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowConfigurationEventStore for InMemoryFlowConfigurationEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_existing_flow_bindings(&self) -> FlowBindingStream {
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

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_scope))]
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
