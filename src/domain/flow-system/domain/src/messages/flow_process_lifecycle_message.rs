// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use chrono::{DateTime, Utc};
use kamu_task_system as ts;
use messaging_outbox::Message;
use serde::{Deserialize, Serialize};

use crate::{FlowBinding, FlowID, FlowProcessAutoStopReason, FlowProcessEffectiveState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FLOW_PROCESS_LIFECYCLE_MESSAGE_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents a message indicating that a flow's process has signifficantly
/// changed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowProcessLifecycleMessage {
    FailureRegistered(FlowProcessFailureRegisteredMessage),
    EffectiveStateChanged(FlowProcessEffectiveStateChangedMessage),
    TriggerAutoStopped(FlowProcessTriggerAutoStoppedMessage),
}

impl Message for FlowProcessLifecycleMessage {
    fn version() -> u32 {
        FLOW_PROCESS_LIFECYCLE_MESSAGE_VERSION
    }
}

impl FlowProcessLifecycleMessage {
    pub fn failure_registered(
        event_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        flow_id: FlowID,
        error: ts::TaskError,
        new_consecutive_failures: u32,
    ) -> Self {
        Self::FailureRegistered(FlowProcessFailureRegisteredMessage {
            event_time,
            flow_binding,
            flow_id,
            error,
            new_consecutive_failures,
        })
    }

    pub fn effective_state_changed(
        event_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        old_effective_state: FlowProcessEffectiveState,
        new_effective_state: FlowProcessEffectiveState,
    ) -> Self {
        Self::EffectiveStateChanged(FlowProcessEffectiveStateChangedMessage {
            event_time,
            flow_binding,
            old_effective_state,
            new_effective_state,
        })
    }

    pub fn trigger_auto_stopped(
        event_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        reason: FlowProcessAutoStopReason,
    ) -> Self {
        Self::TriggerAutoStopped(FlowProcessTriggerAutoStoppedMessage {
            event_time,
            flow_binding,
            reason,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowProcessFailureRegisteredMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The binding of the flow process to which the flow belongs
    pub flow_binding: FlowBinding,

    /// The unique identifier of the flow
    pub flow_id: FlowID,

    /// The associated error outcome
    pub error: ts::TaskError,

    /// Number of consecutive failures for the flow process, including this
    /// failure
    pub new_consecutive_failures: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowProcessEffectiveStateChangedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The binding of the flow process to which the flow belongs
    pub flow_binding: FlowBinding,

    /// The previous effective state
    pub old_effective_state: FlowProcessEffectiveState,

    /// The new effective state
    pub new_effective_state: FlowProcessEffectiveState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]

pub struct FlowProcessTriggerAutoStoppedMessage {
    /// The time at which the event was recorded
    pub event_time: DateTime<Utc>,

    /// The binding of the flow process to which the flow belongs
    pub flow_binding: FlowBinding,

    /// The reason for auto-stopping
    pub reason: FlowProcessAutoStopReason,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
