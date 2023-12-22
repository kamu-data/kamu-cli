// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub enum FlowServiceEvent {
    ConfigurationLoaded(FlowServiceEventConfigurationLoaded),
    ExecutedTimeSlot(FlowServiceEventExecutedTimeSlot),
    FlowFinished(FlowServiceEventFlowFinished),
}

impl FlowServiceEvent {
    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            FlowServiceEvent::ConfigurationLoaded(e) => e.event_time,
            FlowServiceEvent::ExecutedTimeSlot(e) => e.event_time,
            FlowServiceEvent::FlowFinished(e) => e.event_time,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowServiceEventConfigurationLoaded {
    pub event_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowServiceEventExecutedTimeSlot {
    pub event_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct FlowServiceEventFlowFinished {
    pub event_time: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////
