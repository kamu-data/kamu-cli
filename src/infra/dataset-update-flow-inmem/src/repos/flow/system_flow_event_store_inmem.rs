// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use dill::*;
use kamu_dataset_update_flow::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemFlowEventStoreInMem {
    inner: EventStoreInMemory<SystemFlowState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<SystemFlowEvent>,
    flows_by_type: HashMap<SystemFlowType, Vec<SystemFlowID>>,
    last_flow_id: Option<SystemFlowID>,
}

impl State {
    fn next_flow_id(&mut self) -> SystemFlowID {
        let next_flow_id = if let Some(last_flow_id) = self.last_flow_id {
            let id: u64 = last_flow_id.into();
            SystemFlowID::new(id + 1)
        } else {
            SystemFlowID::new(0)
        };
        self.last_flow_id = Some(next_flow_id);
        next_flow_id
    }
}

impl EventStoreState<SystemFlowState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[SystemFlowEvent] {
        &self.events
    }

    fn add_event(&mut self, event: SystemFlowEvent) {
        self.events.push(event);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SystemFlowEventStore)]
#[scope(Singleton)]
impl SystemFlowEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }

    fn update_index_by_flow_type(state: &mut State, event: &SystemFlowEvent) {
        if let SystemFlowEvent::Initiated(e) = &event {
            let entries = match state.flows_by_type.entry(e.flow_key.flow_type) {
                Entry::Occupied(v) => v.into_mut(),
                Entry::Vacant(v) => v.insert(Vec::default()),
            };
            entries.push(event.flow_id());
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<SystemFlowState> for SystemFlowEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events<'a>(
        &'a self,
        query: &SystemFlowID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, SystemFlowEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &SystemFlowID,
        events: Vec<SystemFlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index_by_flow_type(&mut g, &event);
            }
        }

        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SystemFlowEventStore for SystemFlowEventStoreInMem {
    /// Generates new unique flow identifier
    fn new_flow_id(&self) -> SystemFlowID {
        self.inner.as_state().lock().unwrap().next_flow_id()
    }

    /// Returns the last flow of certain type
    fn get_last_specific_flow(&self, flow_type: SystemFlowType) -> Option<SystemFlowID> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        g.flows_by_type
            .get(&flow_type)
            .and_then(|flows| flows.last().cloned())
    }

    /// Returns the flows of certain type in reverse chronological order based
    /// on creation time
    fn get_specific_flows<'a>(&'a self, flow_type: SystemFlowType) -> SystemFlowIDStream<'a> {
        let mut pos = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.flows_by_type
                .get(&flow_type)
                .map(|flows| flows.len())
                .unwrap_or(0)
        };

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let state = self.inner.as_state();
                    let g: std::sync::MutexGuard<'_, State> = state.lock().unwrap();
                    g.flows_by_type
                        .get(&flow_type)
                        .and_then(|flows| flows.get(pos).cloned())
                };

                let flow_id = match next {
                    None => break,
                    Some(flow_id) => flow_id,
                };

                yield flow_id;
            }
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
