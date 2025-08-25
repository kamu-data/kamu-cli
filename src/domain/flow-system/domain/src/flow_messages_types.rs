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

use crate::{
    FlowBinding,
    FlowConfigurationRule,
    FlowID,
    FlowOutcome,
    FlowTriggerRule,
    FlowTriggerStatus,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_AGENT_UPDATE_OUTBOX_VERSION: u32 = 1;
const FLOW_CONFIGURATION_UPDATE_OUTBOX_VERSION: u32 = 2;
const FLOW_TRIGGER_UPDATE_OUTBOX_VERSION: u32 = 3;
const FLOW_PROGRESS_OUTBOX_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow's configuration has been updated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowConfigurationUpdatedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique key identifying the flow
    pub flow_binding: FlowBinding,

    /// The updated configuration rule for the flow
    pub rule: FlowConfigurationRule,
}

impl Message for FlowConfigurationUpdatedMessage {
    fn version() -> u32 {
        FLOW_CONFIGURATION_UPDATE_OUTBOX_VERSION
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow's trigger has been updated
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowTriggerUpdatedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique binding identifying the flow
    pub flow_binding: FlowBinding,

    /// Status of the trigger
    pub trigger_status: FlowTriggerStatus,

    /// The updated trigger rule for the flow
    pub rule: FlowTriggerRule,
}

impl Message for FlowTriggerUpdatedMessage {
    fn version() -> u32 {
        FLOW_TRIGGER_UPDATE_OUTBOX_VERSION
    }
}

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

/// Represents messages related to the progress of a flow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowProgressMessage {
    /// Message indicating that a flow has been scheduled
    Scheduled(FlowProgressMessageScheduled),

    /// Message indicating that a flow is currently running
    Running(FlowProgressMessageRunning),

    /// Message indicating that a flow has finished with a failure,
    /// and a retry is scheduled
    RetryScheduled(FlowProgressMessageRetryScheduled),

    /// Message indicating that a flow has finished execution
    Finished(FlowProgressMessageFinished),

    /// Message indicating that a flow has been cancelled
    Cancelled(FlowProgressMessageCancelled),
}

impl Message for FlowProgressMessage {
    fn version() -> u32 {
        FLOW_PROGRESS_OUTBOX_VERSION
    }
}

impl FlowProgressMessage {
    pub fn scheduled(
        event_time: DateTime<Utc>,
        flow_id: FlowID,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Self {
        Self::Scheduled(FlowProgressMessageScheduled {
            event_time,
            flow_id,
            scheduled_for_activation_at,
        })
    }

    pub fn running(event_time: DateTime<Utc>, flow_id: FlowID) -> Self {
        Self::Running(FlowProgressMessageRunning {
            event_time,
            flow_id,
        })
    }

    pub fn retry_scheduled(
        event_time: DateTime<Utc>,
        flow_id: FlowID,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Self {
        Self::RetryScheduled(FlowProgressMessageRetryScheduled {
            event_time,
            flow_id,
            scheduled_for_activation_at,
        })
    }

    pub fn finished(event_time: DateTime<Utc>, flow_id: FlowID, outcome: FlowOutcome) -> Self {
        Self::Finished(FlowProgressMessageFinished {
            event_time,
            flow_id,
            outcome,
        })
    }

    pub fn cancelled(event_time: DateTime<Utc>, flow_id: FlowID) -> Self {
        Self::Cancelled(FlowProgressMessageCancelled {
            event_time,
            flow_id,
        })
    }
}

/// Contains details about a scheduled flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageScheduled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// The scheduled activation time for the flow
    pub scheduled_for_activation_at: DateTime<Utc>,
}

/// Contains details about a running flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageRunning {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the flow
    pub flow_id: FlowID,
}

/// Contains details about a retrying flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageRetryScheduled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// The scheduled activation time for the flow
    pub scheduled_for_activation_at: DateTime<Utc>,
}

/// Contains details about a finished flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageFinished {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// The outcome of the flow execution
    pub outcome: FlowOutcome,
}

/// Contains details about a cancelled flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageCancelled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier of the flow
    pub flow_id: FlowID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
