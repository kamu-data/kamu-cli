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
pub enum UpdateEvent {
    /// Update initiated
    Initiated(UpdateEventInitiated),
    /// Start condition defined
    StartConditionDefined(UpdateEventStartConditionDefined),
    /// Queued for time
    Queued(UpdateEventQueued),
    /// Secondary triger added
    TriggerAdded(UpdateEventTriggerAdded),
    /// Scheduled/Rescheduled a task
    TaskScheduled(UpdateEventTaskScheduled),
    /// Finished task
    TaskFinished(UpdateEventTaskFinished),
    /// Cancelled update
    Cancelled(UpdateEventCancelled),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventInitiated {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub dataset_id: DatasetID,
    pub trigger: UpdateTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventStartConditionDefined {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub start_condition: UpdateStartCondition,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventQueued {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub activate_at: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventTriggerAdded {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub trigger: UpdateTrigger,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventTaskScheduled {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventTaskFinished {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub task_id: TaskID,
    pub task_outcome: TaskOutcome,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventCancelled {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub by_account_id: AccountID,
    pub by_account_name: AccountName,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl UpdateEvent {
    pub fn update_id(&self) -> UpdateID {
        match self {
            UpdateEvent::Initiated(e) => e.update_id,
            UpdateEvent::StartConditionDefined(e) => e.update_id,
            UpdateEvent::Queued(e) => e.update_id,
            UpdateEvent::TriggerAdded(e) => e.update_id,
            UpdateEvent::TaskScheduled(e) => e.update_id,
            UpdateEvent::TaskFinished(e) => e.update_id,
            UpdateEvent::Cancelled(e) => e.update_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            UpdateEvent::Initiated(e) => &e.event_time,
            UpdateEvent::StartConditionDefined(e) => &e.event_time,
            UpdateEvent::Queued(e) => &e.event_time,
            UpdateEvent::TriggerAdded(e) => &e.event_time,
            UpdateEvent::TaskScheduled(e) => &e.event_time,
            UpdateEvent::TaskFinished(e) => &e.event_time,
            UpdateEvent::Cancelled(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(UpdateEvent);
impl_enum_variant!(UpdateEvent::Initiated(UpdateEventInitiated));
impl_enum_variant!(UpdateEvent::StartConditionDefined(
    UpdateEventStartConditionDefined
));
impl_enum_variant!(UpdateEvent::Queued(UpdateEventQueued));
impl_enum_variant!(UpdateEvent::TriggerAdded(UpdateEventTriggerAdded));
impl_enum_variant!(UpdateEvent::TaskScheduled(UpdateEventTaskScheduled));
impl_enum_variant!(UpdateEvent::TaskFinished(UpdateEventTaskFinished));
impl_enum_variant!(UpdateEvent::Cancelled(UpdateEventCancelled));

/////////////////////////////////////////////////////////////////////////////////////////
