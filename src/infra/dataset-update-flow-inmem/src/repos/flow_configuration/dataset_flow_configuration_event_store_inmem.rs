// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use dill::*;
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigurationEventStoreInMem {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<DatasetFlowConfigurationEvent>,
    dataset_ids: HashSet<DatasetID>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DatasetFlowConfigurationEventStore)]
#[scope(Singleton)]
impl DatasetFlowConfigurationEventStoreInMem {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<DatasetFlowConfigurationState> for DatasetFlowConfigurationEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events.len())
    }

    fn get_events<'a>(
        &'a self,
        query: &DatasetFlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<'a, DatasetFlowConfigurationEvent> {
        let dataset_id = query.dataset_id.clone();
        let flow_type = query.flow_type;

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
                        .filter(|(_, e)| e.flow_key().dataset_id == dataset_id && e.flow_key().flow_type == flow_type)
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
        query: &DatasetFlowKey,
        events: Vec<DatasetFlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let mut s = self.state.lock().unwrap();
        if !events.is_empty() {
            s.dataset_ids.get_or_insert(query.dataset_id.clone());

            for event in events {
                s.events.push(event);
            }
        }

        Ok(EventID::new((s.events.len() - 1) as u64))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetFlowConfigurationEventStore for DatasetFlowConfigurationEventStoreInMem {
    fn list_all_dataset_ids<'a>(&'a self) -> DatasetIDStream<'a> {
        // TODO: re-consider performance impact
        Box::pin(tokio_stream::iter(
            self.state.lock().unwrap().dataset_ids.clone(),
        ))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
