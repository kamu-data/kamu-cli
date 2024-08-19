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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowConfigurationUpdatedMessage {
    pub event_time: DateTime<Utc>,
    pub flow_key: FlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

impl Message for FlowConfigurationUpdatedMessage {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowExecutorUpdatedMessage {
    pub update_time: DateTime<Utc>,
    pub update_details: FlowExecutorUpdateDetails,
}

impl Message for FlowExecutorUpdatedMessage {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowExecutorUpdateDetails {
    Loaded,
    ExecutedTimeslot,
    FlowRunning,
    FlowFinished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
