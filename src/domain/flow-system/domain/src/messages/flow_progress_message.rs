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

use crate::{FlowBinding, FlowID, FlowOutcome};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_PROGRESS_OUTBOX_VERSION: u32 = 2;

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
        event_id: EventID,
        flow_id: FlowID,
        flow_binding: FlowBinding,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Self {
        Self::Scheduled(FlowProgressMessageScheduled {
            event_time,
            event_id,
            flow_id,
            flow_binding,
            scheduled_for_activation_at,
        })
    }

    pub fn running(
        event_time: DateTime<Utc>,
        event_id: EventID,
        flow_id: FlowID,
        flow_binding: FlowBinding,
    ) -> Self {
        Self::Running(FlowProgressMessageRunning {
            event_time,
            event_id,
            flow_id,
            flow_binding,
        })
    }

    pub fn retry_scheduled(
        event_time: DateTime<Utc>,
        event_id: EventID,
        flow_id: FlowID,
        flow_binding: FlowBinding,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Self {
        Self::RetryScheduled(FlowProgressMessageRetryScheduled {
            event_time,
            event_id,
            flow_id,
            flow_binding,
            scheduled_for_activation_at,
        })
    }

    pub fn finished(
        event_time: DateTime<Utc>,
        event_id: EventID,
        flow_id: FlowID,
        flow_binding: FlowBinding,
        outcome: FlowOutcome,
    ) -> Self {
        Self::Finished(FlowProgressMessageFinished {
            event_time,
            event_id,
            flow_id,
            flow_binding,
            outcome,
        })
    }

    pub fn cancelled(
        event_time: DateTime<Utc>,
        event_id: EventID,
        flow_id: FlowID,
        flow_binding: FlowBinding,
    ) -> Self {
        Self::Cancelled(FlowProgressMessageCancelled {
            event_time,
            event_id,
            flow_id,
            flow_binding,
        })
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            FlowProgressMessage::Scheduled(msg) => msg.event_time,
            FlowProgressMessage::Running(msg) => msg.event_time,
            FlowProgressMessage::RetryScheduled(msg) => msg.event_time,
            FlowProgressMessage::Finished(msg) => msg.event_time,
            FlowProgressMessage::Cancelled(msg) => msg.event_time,
        }
    }

    pub fn flow_id(&self) -> &FlowID {
        match self {
            FlowProgressMessage::Scheduled(msg) => &msg.flow_id,
            FlowProgressMessage::Running(msg) => &msg.flow_id,
            FlowProgressMessage::RetryScheduled(msg) => &msg.flow_id,
            FlowProgressMessage::Finished(msg) => &msg.flow_id,
            FlowProgressMessage::Cancelled(msg) => &msg.flow_id,
        }
    }

    pub fn flow_binding(&self) -> &FlowBinding {
        match self {
            FlowProgressMessage::Scheduled(msg) => &msg.flow_binding,
            FlowProgressMessage::Running(msg) => &msg.flow_binding,
            FlowProgressMessage::RetryScheduled(msg) => &msg.flow_binding,
            FlowProgressMessage::Finished(msg) => &msg.flow_binding,
            FlowProgressMessage::Cancelled(msg) => &msg.flow_binding,
        }
    }
}

/// Contains details about a scheduled flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageScheduled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// Flow binding
    pub flow_binding: FlowBinding,

    /// The scheduled activation time for the flow
    pub scheduled_for_activation_at: DateTime<Utc>,
}

/// Contains details about a running flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageRunning {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// Flow binding
    pub flow_binding: FlowBinding,
}

/// Contains details about a retrying flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageRetryScheduled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// Flow binding
    pub flow_binding: FlowBinding,

    /// The scheduled activation time for the flow
    pub scheduled_for_activation_at: DateTime<Utc>,
}

/// Contains details about a finished flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageFinished {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// Flow binding
    pub flow_binding: FlowBinding,

    /// The outcome of the flow execution
    pub outcome: FlowOutcome,
}

/// Contains details about a cancelled flow
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageCancelled {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The unique identifier for the event
    pub event_id: EventID,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// Flow binding
    pub flow_binding: FlowBinding,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
