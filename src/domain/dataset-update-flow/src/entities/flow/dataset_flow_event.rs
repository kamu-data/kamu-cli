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
use opendatafabric::{AccountID, AccountName, DatasetID};

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatasetFlowEvent {
    /// Update initiated
    Initiated(DatasetFlowEventInitiated),
    /// Start condition defined
    StartConditionDefined(DatasetFlowEventStartConditionDefined),
    /// Queued for time
    Queued(DatasetFlowEventQueued),
    /// Secondary triger added
    TriggerAdded(DatasetFlowEventTriggerAdded),
    /// Scheduled/Rescheduled a task
    TaskScheduled(DatasetFlowEventTaskScheduled),
    /// Finished task
    TaskFinished(DatasetFlowEventTaskFinished),
    /// Cancelled update
    Cancelled(DatasetFlowEventCancelled),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventInitiated {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventStartConditionDefined {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub start_condition: FlowStartCondition,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventQueued {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub activate_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventTriggerAdded {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub trigger: FlowTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventTaskScheduled {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventTaskFinished {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowEventCancelled {
    pub event_time: DateTime<Utc>,
    pub flow_id: DatasetFlowID,
    pub by_account_id: AccountID,
    pub by_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetFlowEvent {
    pub fn flow_id(&self) -> DatasetFlowID {
        match self {
            DatasetFlowEvent::Initiated(e) => e.flow_id,
            DatasetFlowEvent::StartConditionDefined(e) => e.flow_id,
            DatasetFlowEvent::Queued(e) => e.flow_id,
            DatasetFlowEvent::TriggerAdded(e) => e.flow_id,
            DatasetFlowEvent::TaskScheduled(e) => e.flow_id,
            DatasetFlowEvent::TaskFinished(e) => e.flow_id,
            DatasetFlowEvent::Cancelled(e) => e.flow_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            DatasetFlowEvent::Initiated(e) => &e.event_time,
            DatasetFlowEvent::StartConditionDefined(e) => &e.event_time,
            DatasetFlowEvent::Queued(e) => &e.event_time,
            DatasetFlowEvent::TriggerAdded(e) => &e.event_time,
            DatasetFlowEvent::TaskScheduled(e) => &e.event_time,
            DatasetFlowEvent::TaskFinished(e) => &e.event_time,
            DatasetFlowEvent::Cancelled(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(DatasetFlowEvent);
impl_enum_variant!(DatasetFlowEvent::Initiated(DatasetFlowEventInitiated));
impl_enum_variant!(DatasetFlowEvent::StartConditionDefined(
    DatasetFlowEventStartConditionDefined
));
impl_enum_variant!(DatasetFlowEvent::Queued(DatasetFlowEventQueued));
impl_enum_variant!(DatasetFlowEvent::TriggerAdded(DatasetFlowEventTriggerAdded));
impl_enum_variant!(DatasetFlowEvent::TaskScheduled(
    DatasetFlowEventTaskScheduled
));
impl_enum_variant!(DatasetFlowEvent::TaskFinished(DatasetFlowEventTaskFinished));
impl_enum_variant!(DatasetFlowEvent::Cancelled(DatasetFlowEventCancelled));

/////////////////////////////////////////////////////////////////////////////////////////
