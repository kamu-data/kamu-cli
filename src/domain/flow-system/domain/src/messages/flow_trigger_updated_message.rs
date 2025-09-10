// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::EventID;
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::{FlowBinding, FlowTriggerRule, FlowTriggerStatus, FlowTriggerStopPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_TRIGGER_UPDATE_OUTBOX_VERSION: u32 = 4;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow's trigger has been updated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTriggerUpdatedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique binding identifying the flow
    pub flow_binding: FlowBinding,

    /// Status of the trigger
    pub trigger_status: FlowTriggerStatus,

    /// The updated trigger rule for the flow
    pub rule: FlowTriggerRule,

    /// The stop policy for the flow
    pub stop_policy: FlowTriggerStopPolicy,

    /// New trigger indicator
    pub is_new_trigger: bool,
}

impl Message for FlowTriggerUpdatedMessage {
    fn version() -> u32 {
        FLOW_TRIGGER_UPDATE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
