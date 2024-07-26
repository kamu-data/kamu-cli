// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use dill::{component, interface, scope, Catalog, Singleton};
use internal_error::InternalError;
use kamu_flow_system::FlowConfigurationUpdatedMessage;
use messaging_outbox::MessageConsumerT;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowConfigTestListener {
    configuration_modified_events: Mutex<Vec<FlowConfigurationUpdatedMessage>>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumerT<FlowConfigurationUpdatedMessage>)]
impl FlowConfigTestListener {
    pub fn new() -> Self {
        Self {
            configuration_modified_events: Mutex::new(Vec::new()),
        }
    }

    pub fn configuration_events_count(&self) -> usize {
        let events = self.configuration_modified_events.lock().unwrap();
        events.len()
    }
}

#[async_trait::async_trait]
impl MessageConsumerT<FlowConfigurationUpdatedMessage> for FlowConfigTestListener {
    #[tracing::instrument(level = "debug", skip_all, fields(?message))]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: FlowConfigurationUpdatedMessage,
    ) -> Result<(), InternalError> {
        let mut events = self.configuration_modified_events.lock().unwrap();
        events.push(message);
        Ok(())
    }

    fn consumer_name(&self) -> &'static str {
        unreachable!("Should not be ever called for test listener");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
