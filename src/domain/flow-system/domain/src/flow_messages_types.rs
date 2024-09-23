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

use crate::{FlowConfigurationRule, FlowID, FlowKey, FlowOutcome};

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FlowProgressMessage {
    Scheduled(FlowProgressMessageScheduled),
    Running(FlowProgressMessageRunning),
    Finished(FlowProgressMessageFinished),
    Cancelled(FlowProgressMessageCancelled),
}

impl Message for FlowProgressMessage {}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageScheduled {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub scheduled_for_activation_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageRunning {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageFinished {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub outcome: FlowOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProgressMessageCancelled {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
