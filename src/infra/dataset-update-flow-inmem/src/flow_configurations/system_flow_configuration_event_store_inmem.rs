// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::*;
use kamu_dataset_update_flow::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemFlowConfigurationEventStoreInMem {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<SystemFlowConfigurationEvent>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SystemFlowConfigurationEventStore)]
#[scope(Singleton)]
impl SystemFlowConfigurationEventStoreInMem {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<SystemFlowConfigurationState> for SystemFlowConfigurationEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        Ok(self.state.lock().unwrap().events.len())
    }

    fn get_events<'a>(
        &'a self,
        query: &SystemFlowType,
        opts: GetEventsOpts,
    ) -> EventStream<'a, SystemFlowConfigurationEvent> {
        let flow_type = *query;

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
                        .filter(|(_, e)| e.flow_type() == flow_type)
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
        _: &SystemFlowType,
        events: Vec<SystemFlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let mut s = self.state.lock().unwrap();
        for event in events {
            s.events.push(event);
        }

        Ok(EventID::new((s.events.len() - 1) as u64))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl SystemFlowConfigurationEventStore for SystemFlowConfigurationEventStoreInMem {}

/////////////////////////////////////////////////////////////////////////////////////////
