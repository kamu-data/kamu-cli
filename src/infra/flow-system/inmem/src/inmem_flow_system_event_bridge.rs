// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeSet, HashMap};
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use kamu_flow_system::*;
use kamu_messaging_outbox_inmem::InMemoryMessageStoreWakeupDetector;
use messaging_outbox::MessageStoreWakeupDetector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowSystemEventBridge {
    state: Mutex<State>,
    wakeup_detector: InMemoryMessageStoreWakeupDetector,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowSystemEvent>,
    // projector name -> applied event ids
    applied: HashMap<&'static str, BTreeSet<EventID>>,
    // projector name -> next scan position in `merged`
    next_pos: HashMap<&'static str, usize>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn FlowSystemEventBridge)]
impl InMemoryFlowSystemEventBridge {
    pub fn new() -> Self {
        let wakeup_detector = InMemoryMessageStoreWakeupDetector::new();

        Self {
            state: Mutex::new(State::default()),
            wakeup_detector,
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

        // Wake up listeners
        self.wakeup_detector.notify_new_message_arrived();

        max_event_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for InMemoryFlowSystemEventBridge {
    /// Provides event store wakeup detector instance
    fn wakeup_detector(&self) -> &dyn MessageStoreWakeupDetector {
        &self.wakeup_detector
    }

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        _: &dill::Catalog,
        projector_name: &'static str,
        batch_size: usize,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let mut state = self.state.lock().unwrap();

        let pos = state.next_pos.get(projector_name).copied().unwrap_or(0);
        let applied = state.applied.entry(projector_name).or_default().clone();

        let mut res = Vec::with_capacity(batch_size);
        let mut i = pos;

        while i < state.events.len() && res.len() < batch_size {
            let e = &state.events[i];

            if !applied.contains(&e.event_id) {
                res.push(e.clone());
            }
            i += 1;
        }

        // Advance the scan cursor to where we stopped scanning.
        // (Safe because we only ever consume in order.)
        state.next_pos.insert(projector_name, i);

        Ok(res)
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

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
