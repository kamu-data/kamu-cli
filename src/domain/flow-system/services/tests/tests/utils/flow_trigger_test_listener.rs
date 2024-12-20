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
use kamu_flow_system::FlowTriggerUpdatedMessage;
use kamu_flow_system_services::MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageDeliveryMechanism,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowTriggerTestListener {
    trigger_modified_events: Mutex<Vec<FlowTriggerUpdatedMessage>>,
}

#[component(pub)]
#[scope(Singleton)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<FlowTriggerUpdatedMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: "FlowTriggerTestListener",
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE],
    delivery: MessageDeliveryMechanism::Immediate,
})]
impl FlowTriggerTestListener {
    pub fn new() -> Self {
        Self {
            trigger_modified_events: Mutex::new(Vec::new()),
        }
    }

    pub fn triggers_events_count(&self) -> usize {
        let events = self.trigger_modified_events.lock().unwrap();
        events.len()
    }
}

impl MessageConsumer for FlowTriggerTestListener {}

#[async_trait::async_trait]
impl MessageConsumerT<FlowTriggerUpdatedMessage> for FlowTriggerTestListener {
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &FlowTriggerUpdatedMessage,
    ) -> Result<(), InternalError> {
        let mut events = self.trigger_modified_events.lock().unwrap();
        events.push(message.clone());
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
