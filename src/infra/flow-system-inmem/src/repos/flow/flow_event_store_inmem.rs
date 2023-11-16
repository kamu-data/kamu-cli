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
use kamu_flow_system::*;
use opendatafabric::DatasetID;

use crate::dataset_flow_key::BorrowedFlowKeyDataset;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowEventStoreInMem {
    inner: EventStoreInMemory<FlowState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowEvent>,
    typed_flows_by_dataset: HashMap<FlowKeyDataset, Vec<FlowID>>,
    all_flows_by_dataset: HashMap<DatasetID, Vec<FlowID>>,
    system_flows_by_type: HashMap<SystemFlowType, Vec<FlowID>>,
    all_system_flows: Vec<FlowID>,
    last_flow_id: Option<FlowID>,
}

impl State {
    fn next_flow_id(&mut self) -> FlowID {
        let next_flow_id = if let Some(last_flow_id) = self.last_flow_id {
            let id: u64 = last_flow_id.into();
            FlowID::new(id + 1)
        } else {
            FlowID::new(0)
        };
        self.last_flow_id = Some(next_flow_id);
        next_flow_id
    }
}

impl EventStoreState<FlowState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[FlowEvent] {
        &self.events
    }

    fn add_event(&mut self, event: FlowEvent) {
        self.events.push(event);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn FlowEventStore)]
#[scope(Singleton)]
impl FlowEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }

    fn update_index(state: &mut State, event: &FlowEvent) {
        if let FlowEvent::Initiated(e) = &event {
            match &e.flow_key {
                FlowKey::Dataset(flow_key) => {
                    let typed_entries = match state.typed_flows_by_dataset.entry(flow_key.clone()) {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    typed_entries.push(event.flow_id());

                    let all_entries = match state
                        .all_flows_by_dataset
                        .entry(flow_key.dataset_id.clone())
                    {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    all_entries.push(event.flow_id());
                }

                FlowKey::System(flow_key) => {
                    let entries = match state.system_flows_by_type.entry(flow_key.flow_type) {
                        Entry::Occupied(v) => v.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::default()),
                    };
                    entries.push(event.flow_id());

                    state.all_system_flows.push(event.flow_id());
                }
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for FlowEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events<'a>(&'a self, query: &FlowID, opts: GetEventsOpts) -> EventStream<'a, FlowEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &FlowID,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index(&mut g, &event);
            }
        }

        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for FlowEventStoreInMem {
    fn new_flow_id(&self) -> FlowID {
        self.inner.as_state().lock().unwrap().next_flow_id()
    }

    fn get_last_dataset_flow_of_type(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<FlowID> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        g.typed_flows_by_dataset
            .get(BorrowedFlowKeyDataset::new(&dataset_id, flow_type).as_trait())
            .and_then(|flows| flows.last().cloned())
    }

    fn get_last_system_flow_of_type(&self, flow_type: SystemFlowType) -> Option<FlowID> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        g.system_flows_by_type
            .get(&flow_type)
            .and_then(|flows| flows.last().cloned())
    }

    fn get_flows_by_dataset_of_type<'a>(
        &'a self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> FlowIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let borrowed_key = BorrowedFlowKeyDataset::new(&dataset_id, flow_type);

            let mut pos = {
                let state = self.inner.as_state();
                let g = state.lock().unwrap();
                g.typed_flows_by_dataset.get(borrowed_key.as_trait()).map(|flows| flows.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let state = self.inner.as_state();
                    let g = state.lock().unwrap();
                    g.typed_flows_by_dataset
                        .get(borrowed_key.as_trait())
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

    fn get_system_flows_of_type<'a>(&'a self, flow_type: SystemFlowType) -> FlowIDStream<'a> {
        let mut pos = {
            let state = self.inner.as_state();
            let g = state.lock().unwrap();
            g.system_flows_by_type
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
                    g.system_flows_by_type
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

    fn get_all_flows_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> FlowIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let mut pos = {
                let state = self.inner.as_state();
                let g = state.lock().unwrap();
                g.all_flows_by_dataset.get(&dataset_id).map(|flows| flows.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let state = self.inner.as_state();
                    let g = state.lock().unwrap();
                    g.all_flows_by_dataset
                        .get(&dataset_id)
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

    fn get_all_system_flows<'a>(&'a self) -> FlowIDStream<'a> {
        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let mut pos = {
                let state = self.inner.as_state();
                let g = state.lock().unwrap();
                g.all_system_flows.len()
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let state = self.inner.as_state();
                    let g = state.lock().unwrap();
                    g.all_system_flows.get(pos).cloned()
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
