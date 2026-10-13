// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use kamu_flow_system::*;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const WAKEUP_CHANNEL: &str = "flow_system_events_ready";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowSystemEventBridge {
    state: Mutex<State>,
    wakeup_hub: Arc<InMemoryWakeupHub>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowSystemEvent>,
    // projector name -> applied event ids
    applied: HashMap<&'static str, BTreeSet<EventID>>,
    // projector name -> length of the applied prefix of `events`
    next_pos: HashMap<&'static str, usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl InMemoryFlowSystemEventBridge {
    pub fn new(wakeup_hub: Arc<InMemoryWakeupHub>) -> Self {
        Self {
            state: Mutex::new(State::default()),
            wakeup_hub,
        }
    }

    pub(crate) fn save_events(
        &self,
        source_type: FlowSystemEventSourceType,
        source_events: &[(DateTime<Utc>, serde_json::Value)],
    ) -> EventID {
        let mut state = self.state.lock().unwrap();

        if source_events.is_empty() {
            return EventID::new(i64::try_from(state.events.len()).unwrap());
        }

        for (occurred_at, payload) in source_events {
            let event_id = state.events.len() + 1;
            let event = FlowSystemEvent {
                event_id: EventID::new(i64::try_from(event_id).unwrap()),
                tx_id: 0, // tx_id, not used in in-mem impl
                source_type,
                occurred_at: *occurred_at,
                payload: payload.clone(),
            };
            state.events.push(event);
        }

        let max_event_id = EventID::new(i64::try_from(state.events.len()).unwrap());

        self.wakeup_hub.signal(WAKEUP_CHANNEL);

        max_event_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for InMemoryFlowSystemEventBridge {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.wakeup_hub.clone(),
            WAKEUP_CHANNEL,
            FLOW_SYSTEM_EVENT_AGENT_NAME,
        ))
    }

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        _: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let state = self.state.lock().unwrap();

        let pos = state.next_pos.get(projector_name).copied().unwrap_or(0);
        let applied = state.applied.get(projector_name);

        // The cursor stays put: a batch that fails to apply is fetched again,
        // as a rolled back transaction would leave it in a database
        Ok(state.events[pos..]
            .iter()
            .filter(|e| applied.is_none_or(|applied| !applied.contains(&e.event_id)))
            .take(batch_size)
            .cloned()
            .collect())
    }

    async fn has_held_back_events(
        &self,
        _transaction_catalog: &dill::Catalog,
    ) -> Result<bool, InternalError> {
        Ok(false)
    }

    /// Mark these events as applied for this projector (idempotent).
    async fn mark_applied(
        &self,
        _: &dill::Catalog,
        projector_name: &'static str,
        event_ids_with_tx_ids: &[(EventID, i64)],
    ) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        // In-memory implementation does not care about
        let set = state.applied.entry(projector_name).or_default();
        for (id, _) in event_ids_with_tx_ids {
            set.insert(*id);
        }

        // Skip the applied prefix on later scans
        let applied = &state.applied[projector_name];
        let mut pos = state.next_pos.get(projector_name).copied().unwrap_or(0);
        while pos < state.events.len() && applied.contains(&state.events[pos].event_id) {
            pos += 1;
        }
        state.next_pos.insert(projector_name, pos);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
