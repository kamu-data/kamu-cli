// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_flow_system::FlowActivationWakeupSource;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Signaled by `InMemoryFlowEventStore` when a flow gets an activation time
pub(crate) const FLOW_ACTIVATION_SCHEDULED_CHANNEL: &str = "flow_activation_scheduled";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryFlowActivationWakeupSource {
    hub: Arc<InMemoryWakeupHub>,
}

#[component(pub)]
#[interface(dyn FlowActivationWakeupSource)]
#[scope(Agnostic)]
impl InMemoryFlowActivationWakeupSource {
    pub fn new(hub: Arc<InMemoryWakeupHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowActivationWakeupSource for InMemoryFlowActivationWakeupSource {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.hub.clone(),
            FLOW_ACTIVATION_SCHEDULED_CHANNEL,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
