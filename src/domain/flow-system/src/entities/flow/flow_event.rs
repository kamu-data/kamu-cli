// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;
use kamu_task_system::{TaskID, TaskOutcome};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowEvent {
    /// Flow initiated
    Initiated(FlowEventInitiated),
    /// Start condition defined
    StartConditionDefined(FlowEventStartConditionDefined),
    /// Queued for time
    Queued(FlowEventQueued),
    /// Secondary triger added
    TriggerAdded(FlowEventTriggerAdded),
    /// Scheduled/Rescheduled a task
    TaskScheduled(FlowEventTaskScheduled),
    /// Task running
    TaskRunning(FlowEventTaskRunning),
    /// Finished task
    TaskFinished(FlowEventTaskFinished),
    /// Aborted flow (system factor, such as dataset delete)
    Aborted(FlowEventAborted),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventInitiated {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub flow_key: FlowKey,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventStartConditionDefined {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub start_condition: FlowStartCondition,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventQueued {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub activate_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTriggerAdded {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTaskScheduled {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTaskRunning {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventTaskFinished {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowEventAborted {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl FlowEvent {
    pub fn flow_id(&self) -> FlowID {
        match self {
            FlowEvent::Initiated(e) => e.flow_id,
            FlowEvent::StartConditionDefined(e) => e.flow_id,
            FlowEvent::Queued(e) => e.flow_id,
            FlowEvent::TriggerAdded(e) => e.flow_id,
            FlowEvent::TaskScheduled(e) => e.flow_id,
            FlowEvent::TaskRunning(e) => e.flow_id,
            FlowEvent::TaskFinished(e) => e.flow_id,
            FlowEvent::Aborted(e) => e.flow_id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            FlowEvent::Initiated(e) => e.event_time,
            FlowEvent::StartConditionDefined(e) => e.event_time,
            FlowEvent::Queued(e) => e.event_time,
            FlowEvent::TriggerAdded(e) => e.event_time,
            FlowEvent::TaskScheduled(e) => e.event_time,
            FlowEvent::TaskRunning(e) => e.event_time,
            FlowEvent::TaskFinished(e) => e.event_time,
            FlowEvent::Aborted(e) => e.event_time,
        }
    }
}

impl_enum_with_variants!(FlowEvent);

impl_enum_variant!(FlowEvent::Initiated(FlowEventInitiated));
impl_enum_variant!(FlowEvent::StartConditionDefined(
    FlowEventStartConditionDefined
));
impl_enum_variant!(FlowEvent::Queued(FlowEventQueued));
impl_enum_variant!(FlowEvent::TriggerAdded(FlowEventTriggerAdded));
impl_enum_variant!(FlowEvent::TaskScheduled(FlowEventTaskScheduled));
impl_enum_variant!(FlowEvent::TaskRunning(FlowEventTaskRunning));
impl_enum_variant!(FlowEvent::TaskFinished(FlowEventTaskFinished));
impl_enum_variant!(FlowEvent::Aborted(FlowEventAborted));

/////////////////////////////////////////////////////////////////////////////////////////
