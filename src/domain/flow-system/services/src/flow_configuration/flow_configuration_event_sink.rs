// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use database_common::TransactionListener;
use dill::*;
use event_bus::{EventBus, EventSink};
use kamu_core::InternalError;
use kamu_flow_system::FlowConfigurationEventModified;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowConfigurationEventSink {
    event_bus: Arc<EventBus>,
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    configuration_modified_events: Vec<FlowConfigurationEventModified>,
}

#[component(pub)]
#[interface(dyn TransactionListener)]
#[interface(dyn EventSink<FlowConfigurationEventModified>)]
impl FlowConfigurationEventSink {
    pub fn new(event_bus: Arc<EventBus>) -> Self {
        Self {
            event_bus,
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl EventSink<FlowConfigurationEventModified> for FlowConfigurationEventSink {
    fn post_event(
        &self,
        event: FlowConfigurationEventModified,
    ) -> Result<(), kamu_core::InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.configuration_modified_events.push(event);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TransactionListener for FlowConfigurationEventSink {
    async fn on_transaction_commit(&self) -> Result<(), InternalError> {
        let configuration_modified_events = {
            let mut guard = self.state.lock().unwrap();
            let mut events = vec![];
            guard
                .configuration_modified_events
                .swap_with_slice(&mut events);
            events
        };

        for event in configuration_modified_events {
            self.event_bus.dispatch_event(event).await?;
        }
        Ok(())
    }

    async fn on_transaction_rollback(&self) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.configuration_modified_events.clear();
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
