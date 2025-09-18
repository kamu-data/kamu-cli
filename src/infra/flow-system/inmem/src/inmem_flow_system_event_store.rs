// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Temporary:
#![allow(dead_code)]

use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::{Singleton, component, scope};
use kamu_flow_system::EventID;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowSystemEventStore {
    time_source: Arc<dyn SystemTimeSource>,
    state: Mutex<State>,
}

#[component(pub)]
#[scope(Singleton)]
impl InMemoryFlowSystemEventStore {
    pub fn new(time_source: Arc<dyn SystemTimeSource>) -> Self {
        Self {
            time_source,
            state: Mutex::new(State::default()),
        }
    }

    pub(crate) fn get_all_events(&self) -> Vec<FlowSystemEvent> {
        let state = self.state.lock().unwrap();
        state.events.clone()
    }

    pub(crate) fn get_all_events_after(&self, after_event_id: EventID) -> Vec<FlowSystemEvent> {
        let state = self.state.lock().unwrap();

        let after_idx = usize::try_from(after_event_id.into_inner()).unwrap() - 1;
        assert!(
            after_idx < state.events.len() || state.events.is_empty(),
            "Invalid after_event_id: {}",
            after_event_id.into_inner()
        );

        state.events.iter().skip(after_idx + 1).copied().collect()
    }

    pub(crate) fn save_events(
        &self,
        source_type: FlowSystemEventSourceType,
        source_events: &[(EventID, DateTime<Utc>)],
    ) {
        let mut state = self.state.lock().unwrap();

        for (source_event_id, occurred_at) in source_events {
            let event_id = state.events.len() + 1;
            let event = FlowSystemEvent {
                event_id: EventID::new(i64::try_from(event_id).unwrap()),
                source_type,
                source_event_id: *source_event_id,
                occurred_at: *occurred_at,
                inserted_at: self.time_source.now(),
            };
            state.events.push(event);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowSystemEvent>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub(crate) struct FlowSystemEvent {
    pub(crate) event_id: EventID,
    pub(crate) source_type: FlowSystemEventSourceType,
    pub(crate) source_event_id: EventID,
    pub(crate) occurred_at: DateTime<Utc>,
    pub(crate) inserted_at: DateTime<Utc>,
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum FlowSystemEventSourceType {
    FlowConfiguration,
    Flow,
    FlowTrigger,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
