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
use opendatafabric::DatasetID;

use crate::dataset_flow_key::{BorrowedDatasetFlowKey, OwnedDatasetFlowKey};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowEventStoreInMem {
    inner: EventStoreInMemory<DatasetFlowState, State>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<DatasetFlowEvent>,
    typed_flows_by_dataset: HashMap<OwnedDatasetFlowKey, Vec<DatasetFlowID>>,
    all_flows_by_dataset: HashMap<DatasetID, Vec<DatasetFlowID>>,
    last_flow_id: Option<DatasetFlowID>,
}

impl State {
    fn next_flow_id(&mut self) -> DatasetFlowID {
        let next_flow_id = if let Some(last_flow_id) = self.last_flow_id {
            let id: u64 = last_flow_id.into();
            DatasetFlowID::new(id + 1)
        } else {
            DatasetFlowID::new(0)
        };
        self.last_flow_id = Some(next_flow_id);
        next_flow_id
    }
}

impl EventStoreState<DatasetFlowState> for State {
    fn events_count(&self) -> usize {
        self.events.len()
    }

    fn get_events(&self) -> &[DatasetFlowEvent] {
        &self.events
    }

    fn add_event(&mut self, event: DatasetFlowEvent) {
        self.events.push(event);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFlowEventStore)]
#[scope(Singleton)]
impl DatasetFlowEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }

    fn update_index_by_dataset(state: &mut State, event: &DatasetFlowEvent) {
        if let DatasetFlowEvent::Initiated(e) = &event {
            let typed_entries = match state.typed_flows_by_dataset.entry(OwnedDatasetFlowKey::new(
                e.flow_key.dataset_id.clone(),
                e.flow_key.flow_type,
            )) {
                Entry::Occupied(v) => v.into_mut(),
                Entry::Vacant(v) => v.insert(Vec::default()),
            };
            typed_entries.push(event.flow_id());

            let all_entries = match state
                .all_flows_by_dataset
                .entry(e.flow_key.dataset_id.clone())
            {
                Entry::Occupied(v) => v.into_mut(),
                Entry::Vacant(v) => v.insert(Vec::default()),
            };
            all_entries.push(event.flow_id());
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<DatasetFlowState> for DatasetFlowEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events<'a>(
        &'a self,
        query: &DatasetFlowID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, DatasetFlowEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &DatasetFlowID,
        events: Vec<DatasetFlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        {
            let state = self.inner.as_state();
            let mut g = state.lock().unwrap();
            for event in &events {
                Self::update_index_by_dataset(&mut g, &event);
            }
        }

        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetFlowEventStore for DatasetFlowEventStoreInMem {
    fn new_flow_id(&self) -> DatasetFlowID {
        self.inner.as_state().lock().unwrap().next_flow_id()
    }

    fn get_last_specific_dataset_flow(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<DatasetFlowID> {
        let state = self.inner.as_state();
        let g = state.lock().unwrap();
        g.typed_flows_by_dataset
            .get(BorrowedDatasetFlowKey::new(&dataset_id, flow_type).as_trait())
            .and_then(|flows| flows.last().cloned())
    }

    fn get_specific_flows_by_dataset<'a>(
        &'a self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> DatasetFlowIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let borrowed_key = BorrowedDatasetFlowKey::new(&dataset_id, flow_type);

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

    fn get_all_flows_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> DatasetFlowIDStream<'a> {
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
}

/////////////////////////////////////////////////////////////////////////////////////////
