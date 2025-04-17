// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use dill::{component, interface, meta, scope, Catalog, Singleton};
use internal_error::InternalError;
use kamu_flow_system::FlowConfigurationUpdatedMessage;
use kamu_flow_system_services::MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE;
use messaging_outbox::{
    InitialConsumerBoundary,
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowConfigTestListener {
    configuration_modified_events: Mutex<Vec<FlowConfigurationUpdatedMessage>>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<FlowConfigurationUpdatedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "FlowConfigTestListener",
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE],
    delivery: MessageDeliveryMechanism::Immediate,
    initial_consumer_boundary: InitialConsumerBoundary::All,
})]
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

impl MessageConsumer for FlowConfigTestListener {}

#[async_trait::async_trait]
impl MessageConsumerT<FlowConfigurationUpdatedMessage> for FlowConfigTestListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &FlowConfigurationUpdatedMessage,
    ) -> Result<(), InternalError> {
        let mut events = self.configuration_modified_events.lock().unwrap();
        events.push(message.clone());
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
