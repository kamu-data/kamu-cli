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

    #[tracing::instrument(level = "debug", skip_all, fields(?opts))]
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<FlowTriggerEvent> {
        self.inner.get_all_events(opts)
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
                FlowTriggerEvent::AutoStopped(_) | FlowTriggerEvent::ScopeRemoved(_) => false,
            };

            if is_active {
                active_bindings.push(flow_binding.clone());
            }
        }

        // Convert into stream
        Box::pin(futures::stream::iter(active_bindings.into_iter().map(Ok)))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_scope))]
    async fn all_trigger_bindings_for_scope(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let mut seen_flow_types = HashSet::new();
        let mut bindings = Vec::new();

        for event in g.events.iter().rev() {
            let binding = event.flow_binding();
            if binding.scope != *flow_scope {
                continue;
            }
            if seen_flow_types.insert(binding.flow_type.clone()) {
                bindings.push(binding.clone());
            }
        }

        Ok(bindings)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError> {
        if scopes.is_empty() {
            return Ok(false);
        }

        let state = self.inner.as_state();
        let g = state.lock().unwrap();

        let scopes: HashSet<&FlowScope> = scopes.iter().collect();
        let mut seen_bindings = HashSet::new();

        for event in g.events.iter().rev() {
            // Skip if we've already seen this binding (we only want the latest event)
            let flow_binding = event.flow_binding();
            if !seen_bindings.insert(flow_binding) {
                continue;
            }

            if scopes.contains(&flow_binding.scope) {
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
                    FlowTriggerEvent::AutoStopped(_) | FlowTriggerEvent::ScopeRemoved(_) => {
                        // auto-stopped or permanently stopped — not active
                    }
                }
            }
        }

        Ok(false)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
