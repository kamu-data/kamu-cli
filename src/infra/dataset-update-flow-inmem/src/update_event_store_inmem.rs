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

use dill::{component, scope, Singleton};
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UpdateEventStoreInMem {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<UpdateEvent>,
    updates_by_dataset: HashMap<DatasetID, Vec<UpdateID>>,
    last_update_id: Option<UpdateID>,
}

impl State {
    fn next_update_id(&mut self) -> UpdateID {
        let next_update_id = if let Some(last_update_id) = self.last_update_id {
            let id: u64 = last_update_id.into();
            UpdateID::new(id + 1)
        } else {
            UpdateID::new(0)
        };
        self.last_update_id = Some(next_update_id);
        next_update_id
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
impl UpdateEventStoreInMem {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    fn update_index_by_dataset(
        updates_by_dataset: &mut HashMap<DatasetID, Vec<UpdateID>>,
        event: &UpdateEvent,
    ) {
        if let UpdateEvent::Initiated(e) = &event {
            let entries = match updates_by_dataset.entry(e.dataset_id.clone()) {
                Entry::Occupied(v) => v.into_mut(),
                Entry::Vacant(v) => v.insert(Vec::default()),
            };
            entries.push(event.update_id())
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<UpdateState> for UpdateEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events.len())
    }

    fn get_events<'a>(
        &'a self,
        update_id: &UpdateID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, UpdateEvent> {
        let update_id = update_id.clone();

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
                        .filter(|(_, e)| e.update_id() == update_id)
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
    async fn save_events(&self, events: Vec<UpdateEvent>) -> Result<EventID, SaveEventsError> {
        let mut s = self.state.lock().unwrap();

        for event in events {
            Self::update_index_by_dataset(&mut s.updates_by_dataset, &event);
            s.events.push(event);
        }

        Ok(EventID::new((s.events.len() - 1) as u64))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl UpdateEventStore for UpdateEventStoreInMem {
    fn new_update_id(&self) -> UpdateID {
        self.state.lock().unwrap().next_update_id()
    }

    fn get_updates_by_dataset<'a>(&'a self, dataset_id: &DatasetID) -> UpdateIDStream<'a> {
        let dataset_id = dataset_id.clone();

        // TODO: This should be a buffered stream so we don't lock per record
        Box::pin(async_stream::try_stream! {
            let mut pos = {
                let s = self.state.lock().unwrap();
                s.updates_by_dataset.get(&dataset_id).map(|updates| updates.len()).unwrap_or(0)
            };

            loop {
                if pos == 0 {
                    break;
                }

                pos -= 1;

                let next = {
                    let s = self.state.lock().unwrap();
                    s.updates_by_dataset
                        .get(&dataset_id)
                        .and_then(|updates| updates.get(pos).cloned())
                };

                let update_id = match next {
                    None => break,
                    Some(update_id) => update_id,
                };

                yield update_id;
            }
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
