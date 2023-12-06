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
use opendatafabric::{AccountID, AccountName};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SystemFlowEvent {
    /// Update initiated
    Initiated(SystemFlowEventInitiated),
    /// Start condition defined
    StartConditionDefined(SystemFlowEventStartConditionDefined),
    /// Queued for time
    Queued(SystemFlowEventQueued),
    /// Secondary triger added
    TriggerAdded(SystemFlowEventTriggerAdded),
    /// Scheduled/Rescheduled a task
    TaskScheduled(SystemFlowEventTaskScheduled),
    /// Finished task
    TaskFinished(SystemFlowEventTaskFinished),
    /// Cancelled update
    Cancelled(SystemFlowEventCancelled),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventInitiated {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub flow_type: SystemFlowType,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventStartConditionDefined {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub start_condition: FlowStartCondition,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventQueued {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub activate_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventTriggerAdded {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventTaskScheduled {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventTaskFinished {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowEventCancelled {
    pub event_time: DateTime<Utc>,
    pub flow_id: SystemFlowID,
    pub by_account_id: AccountID,
    pub by_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl SystemFlowEvent {
    pub fn flow_id(&self) -> SystemFlowID {
        match self {
            SystemFlowEvent::Initiated(e) => e.flow_id,
            SystemFlowEvent::StartConditionDefined(e) => e.flow_id,
            SystemFlowEvent::Queued(e) => e.flow_id,
            SystemFlowEvent::TriggerAdded(e) => e.flow_id,
            SystemFlowEvent::TaskScheduled(e) => e.flow_id,
            SystemFlowEvent::TaskFinished(e) => e.flow_id,
            SystemFlowEvent::Cancelled(e) => e.flow_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            SystemFlowEvent::Initiated(e) => &e.event_time,
            SystemFlowEvent::StartConditionDefined(e) => &e.event_time,
            SystemFlowEvent::Queued(e) => &e.event_time,
            SystemFlowEvent::TriggerAdded(e) => &e.event_time,
            SystemFlowEvent::TaskScheduled(e) => &e.event_time,
            SystemFlowEvent::TaskFinished(e) => &e.event_time,
            SystemFlowEvent::Cancelled(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(SystemFlowEvent);
impl_enum_variant!(SystemFlowEvent::Initiated(SystemFlowEventInitiated));
impl_enum_variant!(SystemFlowEvent::StartConditionDefined(
    SystemFlowEventStartConditionDefined
));
impl_enum_variant!(SystemFlowEvent::Queued(SystemFlowEventQueued));
impl_enum_variant!(SystemFlowEvent::TriggerAdded(SystemFlowEventTriggerAdded));
impl_enum_variant!(SystemFlowEvent::TaskScheduled(SystemFlowEventTaskScheduled));
impl_enum_variant!(SystemFlowEvent::TaskFinished(SystemFlowEventTaskFinished));
impl_enum_variant!(SystemFlowEvent::Cancelled(SystemFlowEventCancelled));

/////////////////////////////////////////////////////////////////////////////////////////
