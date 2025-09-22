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

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use kamu_flow_system::*;
use time_source::SystemTimeSource;
use tokio::sync::broadcast;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowSystemEventBridge {
    time_source: Arc<dyn SystemTimeSource>,
    state: Mutex<State>,
    tx: broadcast::Sender<EventID>,
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
    pub fn new(time_source: Arc<dyn SystemTimeSource>) -> Self {
        let (tx, _rx) = broadcast::channel(1024);

        Self {
            time_source,
            state: Mutex::new(State::default()),
            tx,
        }
    }

    pub(crate) fn get_events_count(&self) -> usize {
        let state = self.state.lock().unwrap();
        state.events.len()
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

        state.events.iter().skip(after_idx + 1).cloned().collect()
    }

    pub(crate) fn save_events(
        &self,
        source_type: FlowSystemEventSourceType,
        source_events: &[(EventID, DateTime<Utc>, serde_json::Value)],
    ) {
        let mut state = self.state.lock().unwrap();

        for (source_event_id, occurred_at, payload) in source_events {
            let event_id = state.events.len() + 1;
            let event = FlowSystemEvent {
                event_id: EventID::new(i64::try_from(event_id).unwrap()),
                source_type,
                source_event_id: *source_event_id,
                occurred_at: *occurred_at,
                inserted_at: self.time_source.now(),
                payload: payload.clone(),
            };
            state.events.push(event);
        }

        let max_event_id = state.events.len();

        // Wake up listeners
        let _ = self
            .tx
            .send(EventID::new(i64::try_from(max_event_id).unwrap()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventBridge for InMemoryFlowSystemEventBridge {
    async fn wait_wake(
        &self,
        timeout: Duration,
        _min_debounce_interval: Duration,
    ) -> Result<FlowSystemEventStoreWakeHint, InternalError> {
        // Subscribe to events broadcast channel
        let mut rx = self.tx.subscribe();

        // Wait until a new event arrives or timeout elapses
        // For testing purposes, we keep this simple without complex backoff strategies
        match tokio::time::timeout(timeout, rx.recv()).await {
            Ok(Ok(event_id)) => {
                // New event arrived
                Ok(FlowSystemEventStoreWakeHint::NewEvents {
                    upper_event_id_bound: event_id,
                })
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                // Sender has been dropped, which should never happen in this case
                unreachable!("InMemoryFlowSystemEventStore: broadcast channel closed");
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                // We lagged behind, but that's fine, just indicate new events are available
                Ok(FlowSystemEventStoreWakeHint::NewEvents {
                    upper_event_id_bound: EventID::new(
                        i64::try_from(self.get_events_count()).unwrap(),
                    ),
                })
            }
            Err(_elapsed) => {
                // Timeout elapsed
                Ok(FlowSystemEventStoreWakeHint::Timeout)
            }
        }
    }

    /// Fetch next batch for the given projector; order by global id.
    async fn fetch_next_batch(
        &self,
        _: &dill::Catalog,
        projector_name: &'static str,
        limit: usize,
        maybe_upper_event_id_bound: Option<EventID>,
    ) -> Result<Vec<FlowSystemEvent>, InternalError> {
        let mut state = self.state.lock().unwrap();

        let pos = state.next_pos.get(projector_name).copied().unwrap_or(0);
        let applied = state.applied.entry(projector_name).or_default().clone();

        let mut res = Vec::with_capacity(limit);
        let mut i = pos;

        while i < state.events.len() && res.len() < limit {
            let e = &state.events[i];

            if let Some(upper_bound) = maybe_upper_event_id_bound
                && e.event_id > upper_bound
            {
                break; // don’t scan beyond the hint
            }

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
        event_ids: &[EventID],
    ) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

        let set = state.applied.entry(projector_name).or_default();
        for &id in event_ids {
            set.insert(id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
