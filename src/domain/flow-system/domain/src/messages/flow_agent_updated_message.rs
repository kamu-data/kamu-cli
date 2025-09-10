// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_AGENT_UPDATE_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow agent has been updated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowAgentUpdatedMessage {
    /// The time at which the update occurred
    pub update_time: DateTime<Utc>,

    /// The details of the agent update.
    pub update_details: FlowAgentUpdateDetails,
}

impl Message for FlowAgentUpdatedMessage {
    fn version() -> u32 {
        FLOW_AGENT_UPDATE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents different types of updates to a flow agent
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowAgentUpdateDetails {
    /// Indicates that the agent has been loaded
    Loaded,

    /// Indicates that the agent has executed a timeslot
    ExecutedTimeslot,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
