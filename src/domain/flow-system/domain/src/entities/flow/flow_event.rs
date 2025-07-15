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
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowEvent {
    /// Flow initiated
    Initiated(FlowEventInitiated),
    /// Start condition updated
    StartConditionUpdated(FlowEventStartConditionUpdated),
    /// Config snapshot modified
    ConfigSnapshotModified(FlowConfigSnapshotModified),
    /// Secondary activation cause added
    ActivationCauseAdded(FlowEventActivationCauseAdded),
    /// Scheduled for activation at a particular time
    ScheduledForActivation(FlowEventScheduledForActivation),
    /// Scheduled/Rescheduled a task
    TaskScheduled(FlowEventTaskScheduled),
    /// Task running
    TaskRunning(FlowEventTaskRunning),
    /// Finished task
    TaskFinished(FlowEventTaskFinished),
    /// Aborted flow (system factor, such as dataset delete)
    Aborted(FlowEventAborted),
}

impl FlowEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            FlowEvent::Initiated(_) => "FlowEventInitiated",
            FlowEvent::StartConditionUpdated(_) => "FlowEventStartConditionUpdated",
            FlowEvent::ConfigSnapshotModified(_) => "FlowEventConfigSnapshotModified",
            FlowEvent::ActivationCauseAdded(_) => "FlowEventActivationCauseAdded",
            FlowEvent::ScheduledForActivation(_) => "FlowEventScheduledForActivation",
            FlowEvent::TaskScheduled(_) => "FlowEventTaskScheduled",
            FlowEvent::TaskRunning(_) => "FlowEventTaskRunning",
            FlowEvent::TaskFinished(_) => "FlowEventTaskFinished",
            FlowEvent::Aborted(_) => "FlowEventAborted",
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventInitiated {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub flow_binding: FlowBinding,
    pub activation_cause: FlowActivationCause,
    pub config_snapshot: Option<FlowConfigurationRule>,
    pub retry_policy: Option<RetryPolicy>,
    pub run_arguments: Option<FlowRunArguments>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventStartConditionUpdated {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub start_condition: FlowStartCondition,
    pub last_activation_cause_index: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventActivationCauseAdded {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub activation_cause: FlowActivationCause,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigSnapshotModified {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub config_snapshot: FlowConfigurationRule,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventScheduledForActivation {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub scheduled_for_activation_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventTaskScheduled {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventTaskRunning {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventTaskFinished {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
    #[serde(default)]
    pub next_attempt_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowEventAborted {
    pub event_time: DateTime<Utc>,
    pub flow_id: FlowID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowEvent {
    pub fn flow_id(&self) -> FlowID {
        match self {
            FlowEvent::Initiated(e) => e.flow_id,
            FlowEvent::StartConditionUpdated(e) => e.flow_id,
            FlowEvent::ConfigSnapshotModified(e) => e.flow_id,
            FlowEvent::ActivationCauseAdded(e) => e.flow_id,
            FlowEvent::ScheduledForActivation(e) => e.flow_id,
            FlowEvent::TaskScheduled(e) => e.flow_id,
            FlowEvent::TaskRunning(e) => e.flow_id,
            FlowEvent::TaskFinished(e) => e.flow_id,
            FlowEvent::Aborted(e) => e.flow_id,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            FlowEvent::Initiated(e) => e.event_time,
            FlowEvent::StartConditionUpdated(e) => e.event_time,
            FlowEvent::ConfigSnapshotModified(e) => e.event_time,
            FlowEvent::ActivationCauseAdded(e) => e.event_time,
            FlowEvent::ScheduledForActivation(e) => e.event_time,
            FlowEvent::TaskScheduled(e) => e.event_time,
            FlowEvent::TaskRunning(e) => e.event_time,
            FlowEvent::TaskFinished(e) => e.event_time,
            FlowEvent::Aborted(e) => e.event_time,
        }
    }

    pub fn new_status(&self) -> Option<FlowStatus> {
        match self {
            FlowEvent::Initiated(_) => Some(FlowStatus::Waiting),
            FlowEvent::StartConditionUpdated(_)
            | FlowEvent::ConfigSnapshotModified(_)
            | FlowEvent::ActivationCauseAdded(_)
            | FlowEvent::ScheduledForActivation(_)
            | FlowEvent::TaskScheduled(_) => None,
            FlowEvent::TaskRunning(_) => Some(FlowStatus::Running),
            FlowEvent::TaskFinished(e) => {
                if e.next_attempt_at.is_some() {
                    Some(FlowStatus::Retrying)
                } else {
                    Some(FlowStatus::Finished)
                }
            }
            FlowEvent::Aborted(_) => Some(FlowStatus::Finished),
        }
    }
}

impl_enum_with_variants!(FlowEvent);

impl_enum_variant!(FlowEvent::Initiated(FlowEventInitiated));
impl_enum_variant!(FlowEvent::StartConditionUpdated(
    FlowEventStartConditionUpdated
));
impl_enum_variant!(FlowEvent::ConfigSnapshotModified(
    FlowConfigSnapshotModified
));
impl_enum_variant!(FlowEvent::ActivationCauseAdded(
    FlowEventActivationCauseAdded
));
impl_enum_variant!(FlowEvent::ScheduledForActivation(
    FlowEventScheduledForActivation
));
impl_enum_variant!(FlowEvent::TaskScheduled(FlowEventTaskScheduled));
impl_enum_variant!(FlowEvent::TaskRunning(FlowEventTaskRunning));
impl_enum_variant!(FlowEvent::TaskFinished(FlowEventTaskFinished));
impl_enum_variant!(FlowEvent::Aborted(FlowEventAborted));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
