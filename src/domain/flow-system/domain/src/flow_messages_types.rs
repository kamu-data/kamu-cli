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
pub const MESSAGE_KAMU_FLOW_SERVICE_UPDATED: &str = "dev.kamu.flow.service.updated";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
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
pub struct FlowServiceUpdatedMessage {
    pub update_time: DateTime<Utc>,
    pub update_details: FlowServiceUpdateDetails,
}

impl Message for FlowServiceUpdatedMessage {
    fn type_name(&self) -> &'static str {
        MESSAGE_KAMU_FLOW_SERVICE_UPDATED
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowServiceUpdateDetails {
    Loaded,
    ExecutedTimeslot,
    FlowRunning,
    FlowFinished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
