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
use std::time::Duration;

use chrono::{DateTime, Utc};
use kamu_flow_system::{
    EventID,
    FlowSystemEventStore,
    FlowSystemEventStoreWakeHint,
    InternalError,
};
use time_source::SystemTimeSource;
use tokio::sync::broadcast;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowSystemEventStore {
    time_source: Arc<dyn SystemTimeSource>,
    state: Mutex<State>,
    tx: broadcast::Sender<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    events: Vec<FlowSystemEvent>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::scope(dill::Singleton)]
#[dill::interface(dyn FlowSystemEventStore)]
impl InMemoryFlowSystemEventStore {
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

        let max_event_id = state.events.len();

        // Wake up listeners
        let _ = self
            .tx
            .send(EventID::new(i64::try_from(max_event_id).unwrap()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventStore for InMemoryFlowSystemEventStore {
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
