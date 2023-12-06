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
use std::sync::{Arc, Mutex};

use dill::*;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

use crate::dataset_flow_key::{BorrowedDatasetFlowKey, OwnedDatasetFlowKey};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowEventStoreInMem {
    state: Arc<Mutex<State>>,
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

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFlowEventStore)]
#[scope(Singleton)]
impl DatasetFlowEventStoreInMem {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn update_index_by_dataset(state: &mut State, event: &DatasetFlowEvent) {
        if let DatasetFlowEvent::Initiated(e) = &event {
            let typed_entries = match state
                .typed_flows_by_dataset
                .entry(OwnedDatasetFlowKey::new(e.dataset_id.clone(), e.flow_type))
            {
                Entry::Occupied(v) => v.into_mut(),
                Entry::Vacant(v) => v.insert(Vec::default()),
            };
            typed_entries.push(event.flow_id());

            let all_entries = match state.all_flows_by_dataset.entry(e.dataset_id.clone()) {
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
        Ok(self.state.lock().unwrap().events.len())
    }

    fn get_events<'a>(
        &'a self,
        flow_id: &DatasetFlowID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, DatasetFlowEvent> {
        let flow_id = flow_id.clone();

        // TODO: This should be a buffered stream so we don't lock per event
        Box::pin(async_stream::try_stream! {
            let mut seen = opts.from.map(|id| (id.into_inner() + 1) as usize).unwrap_or(0);

            loop {
                let next = {
                    let s = self.state.lock().unwrap();

                    let to = opts.to.map(|id| (id.into_inner() + 1) as usize).unwrap_or(s.events.len());

                    s.events[..to]
                        .iter()
                        .enumerate()
                        .skip(seen)
                        .filter(|(_, e)| e.flow_id() == flow_id)
                        .map(|(i, e)| (i, e.clone()))
                        .next()
                };

                match next {
                    None => break,
                    Some((i, event)) => {
                        seen = i + 1;
                        yield (EventID::new(i as u64), event)
                    }
                }
            }
        })
    }

    // TODO: concurrency
    async fn save_events(
        &self,
        _flow_id: &DatasetFlowID,
        events: Vec<DatasetFlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let mut s = self.state.lock().unwrap();

        for event in events {
            Self::update_index_by_dataset(&mut s, &event);
            s.events.push(event);
        }

        Ok(EventID::new((s.events.len() - 1) as u64))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetFlowEventStore for DatasetFlowEventStoreInMem {
    fn new_flow_id(&self) -> DatasetFlowID {
        self.state.lock().unwrap().next_flow_id()
    }

    fn get_last_specific_dataset_flow(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Option<DatasetFlowID> {
        let s = self.state.lock().unwrap();
        s.typed_flows_by_dataset
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
                let s = self.state.lock().unwrap();
                s.typed_flows_by_dataset.get(borrowed_key.as_trait()).map(|flows| flows.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let s = self.state.lock().unwrap();
                    s.typed_flows_by_dataset
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
                let s = self.state.lock().unwrap();
                s.all_flows_by_dataset.get(&dataset_id).map(|flows| flows.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let s = self.state.lock().unwrap();
                    s.all_flows_by_dataset
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
