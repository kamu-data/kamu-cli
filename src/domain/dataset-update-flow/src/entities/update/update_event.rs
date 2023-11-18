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
use kamu_task_system::TaskID;
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a task
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateEvent {
    /// Update initiated
    Initiated(UpdateEventInitiated),
    /// Postponed
    Postponed(UpdateEventPostponed),
    /// Queued for time
    Queued(UpdateEventQueued),
    /// Secondary triger
    SecondaryTrigger(UpdateEventSecondaryTrigger),
    /// Scheduled/Rescheduled a task
    TaskScheduled(UpdateEventTaskScheduled),
    /// Succeeded task
    TaskSucceeded(UpdateEventTaskSucceeded),
    /// Failed task
    TaskFailed(UpdateEventTaskFailed),
    /// Cancelled task
    TaskCancelled(UpdateEventTaskCancelled),
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
pub struct UpdateEventPostponed {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub delay_reason: UpdateDelayReason,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventQueued {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub queued_for: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventSecondaryTrigger {
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
pub struct UpdateEventTaskSucceeded {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventTaskFailed {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateEventTaskCancelled {
    pub event_time: DateTime<Utc>,
    pub update_id: UpdateID,
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl UpdateEvent {
    pub fn update_id(&self) -> UpdateID {
        match self {
            UpdateEvent::Initiated(e) => e.update_id,
            UpdateEvent::Postponed(e) => e.update_id,
            UpdateEvent::Queued(e) => e.update_id,
            UpdateEvent::SecondaryTrigger(e) => e.update_id,
            UpdateEvent::TaskScheduled(e) => e.update_id,
            UpdateEvent::TaskSucceeded(e) => e.update_id,
            UpdateEvent::TaskFailed(e) => e.update_id,
            UpdateEvent::TaskCancelled(e) => e.update_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            UpdateEvent::Initiated(e) => &e.event_time,
            UpdateEvent::Postponed(e) => &e.event_time,
            UpdateEvent::Queued(e) => &e.event_time,
            UpdateEvent::SecondaryTrigger(e) => &e.event_time,
            UpdateEvent::TaskScheduled(e) => &e.event_time,
            UpdateEvent::TaskSucceeded(e) => &e.event_time,
            UpdateEvent::TaskFailed(e) => &e.event_time,
            UpdateEvent::TaskCancelled(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(UpdateEvent);
impl_enum_variant!(UpdateEvent::Initiated(UpdateEventInitiated));
impl_enum_variant!(UpdateEvent::Postponed(UpdateEventPostponed));
impl_enum_variant!(UpdateEvent::Queued(UpdateEventQueued));
impl_enum_variant!(UpdateEvent::SecondaryTrigger(UpdateEventSecondaryTrigger));
impl_enum_variant!(UpdateEvent::TaskScheduled(UpdateEventTaskScheduled));
impl_enum_variant!(UpdateEvent::TaskSucceeded(UpdateEventTaskSucceeded));
impl_enum_variant!(UpdateEvent::TaskFailed(UpdateEventTaskFailed));
impl_enum_variant!(UpdateEvent::TaskCancelled(UpdateEventTaskCancelled));

/////////////////////////////////////////////////////////////////////////////////////////
