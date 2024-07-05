// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use database_common::TransactionListener;
use dill::*;
use event_bus::{EventBus, TransactionEventSink};
use kamu_core::InternalError;
use kamu_flow_system::FlowConfigurationEventModified;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationEventSink {
    event_bus: Arc<EventBus>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    events_by_transaction_id: HashMap<Uuid, TransactionEvents>,
}

struct TransactionEvents {
    configuration_modified: Vec<FlowConfigurationEventModified>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn TransactionListener)]
#[interface(dyn TransactionEventSink<FlowConfigurationEventModified>)]
impl FlowConfigurationEventSink {
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self {
            event_bus,
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TransactionEventSink<FlowConfigurationEventModified> for FlowConfigurationEventSink {
    fn post_event(
        &self,
        transaction_id: &Uuid,
        event: FlowConfigurationEventModified,
    ) -> Result<(), kamu_core::InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard
            .events_by_transaction_id
            .entry(*transaction_id)
            .and_modify(|transaction_events| {
                transaction_events
                    .configuration_modified
                    .push(event.clone())
            })
            .or_insert(TransactionEvents {
                configuration_modified: vec![event],
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransactionListener for FlowConfigurationEventSink {
    async fn on_transaction_commit(&self, transaction_id: &Uuid) -> Result<(), InternalError> {
        let maybe_events = {
            let mut guard = self.state.lock().unwrap();
            guard.events_by_transaction_id.remove(transaction_id)
        };

        if let Some(events) = maybe_events {
            for event in events.configuration_modified {
                self.event_bus.dispatch_event(event).await?;
            }
        }

        Ok(())
    }

    async fn on_transaction_rollback(&self, transaction_id: &Uuid) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.events_by_transaction_id.remove(transaction_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
