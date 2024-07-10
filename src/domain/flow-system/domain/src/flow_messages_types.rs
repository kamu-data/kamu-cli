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

use crate::{FlowConfigurationRule, FlowKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED: &str = "dev.kamu.flow.configuration.updated";

pub const MESSAGE_KAMU_FLOW_SERVICE_LOADED: &str = "dev.kamu.flow.service.loaded";
pub const MESSAGE_KAMU_FLOW_SERVICE_EXECUTED_TIMESLOT: &str =
    "dev.kamu.flow.service.executed_timeslot";
pub const MESSAGE_KAMU_FLOW_SERVICE_RUNNING_FLOW: &str = "dev.kamu.flow.service.running_flow";
pub const MESSAGE_KAMU_FLOW_SERVICE_FINISHED_FLOW: &str = "dev.kamu.flow.service.finished_flow";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigurationUpdatedMessage {
    pub event_time: DateTime<Utc>,
    pub flow_key: FlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

impl Message for FlowConfigurationUpdatedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_CONFIGURATION_UPDATED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowServiceLoadedMessage {
    pub event_time: DateTime<Utc>,
}

impl Message for FlowServiceLoadedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_SERVICE_LOADED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowServiceExecutedTimeSlotMessage {
    pub event_time: DateTime<Utc>,
}

impl Message for FlowServiceExecutedTimeSlotMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_SERVICE_EXECUTED_TIMESLOT
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowServiceFlowRunningMessage {
    pub event_time: DateTime<Utc>,
}

impl Message for FlowServiceFlowRunningMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_SERVICE_RUNNING_FLOW
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowServiceFlowFinishedMessage {
    pub event_time: DateTime<Utc>,
}

impl Message for FlowServiceFlowFinishedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_SERVICE_FINISHED_FLOW
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
