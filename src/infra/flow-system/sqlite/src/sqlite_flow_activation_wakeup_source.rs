// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_flow_system::{FLOW_AGENT_NAME, FlowActivationWakeupSource};
use kamu_wakeup_listener_sqlite::{SqlitePollingChannel, SqlitePollingHub};
use wakeup_listener::{HubWakeupListener, WakeupListener};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const POLLING_CHANNEL: SqlitePollingChannel = SqlitePollingChannel {
    name: "flow_activation_scheduled",
    // Scanning back by primary key finds a match within a few rows, as nearly
    // every flow gets scheduled; finished tasks set a time only for retries
    max_id_query: r#"
        SELECT (
            SELECT event_id FROM flow_events
                WHERE event_type = 'FlowEventScheduledForActivation'
                    OR (
                        event_type = 'FlowEventTaskFinished'
                        AND json_extract(event_payload, '$.TaskFinished.next_attempt_at') IS NOT NULL
                    )
                ORDER BY event_id DESC
                LIMIT 1
        )
    "#,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteFlowActivationWakeupSource {
    hub: Arc<SqlitePollingHub>,
}

#[dill::component(pub)]
#[dill::scope(dill::scopes::Agnostic)]
#[dill::interface(dyn FlowActivationWakeupSource)]
impl SqliteFlowActivationWakeupSource {
    pub fn new(hub: Arc<SqlitePollingHub>) -> Self {
        Self { hub }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowActivationWakeupSource for SqliteFlowActivationWakeupSource {
    fn new_wakeup_listener(&self) -> Box<dyn WakeupListener> {
        Box::new(HubWakeupListener::new(
            self.hub.clone(),
            POLLING_CHANNEL,
            FLOW_AGENT_NAME,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
