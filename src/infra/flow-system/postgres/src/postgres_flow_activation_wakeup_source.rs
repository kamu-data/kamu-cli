// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_flow_system::FlowActivationWakeupSource;
use kamu_wakeup_listener_postgres::PostgresNotificationHub;
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const NOTIFY_CHANNEL_NAME: &str = "flow_activation_scheduled";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowActivationWakeupSource {
    hub: Arc<PostgresNotificationHub>,
}

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn FlowActivationWakeupSource)]
impl PostgresFlowActivationWakeupSource {
    pub fn new(hub: Arc<PostgresNotificationHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowActivationWakeupSource for PostgresFlowActivationWakeupSource {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.hub.clone(),
            NOTIFY_CHANNEL_NAME,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
