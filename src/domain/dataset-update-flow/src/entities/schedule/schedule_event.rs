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
use opendatafabric::DatasetID;

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// All events that model life-cycle of a task
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateScheduleEvent {
    ScheduleCreated(UpdateScheduleCreated),
    SchedulePaused(UpdateSchedulePaused),
    ScheduleResumed(UpdateScheduleResumed),
    ScheduleModified(UpdateScheduleModified),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleCreated {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub schedule: ScheduleType,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSchedulePaused {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleResumed {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleModified {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub new_schedule: ScheduleType,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl UpdateScheduleEvent {
    pub fn dataset_id(&self) -> &DatasetID {
        match self {
            UpdateScheduleEvent::ScheduleCreated(e) => &e.dataset_id,
            UpdateScheduleEvent::SchedulePaused(e) => &e.dataset_id,
            UpdateScheduleEvent::ScheduleResumed(e) => &e.dataset_id,
            UpdateScheduleEvent::ScheduleModified(e) => &e.dataset_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            UpdateScheduleEvent::ScheduleCreated(e) => &e.event_time,
            UpdateScheduleEvent::SchedulePaused(e) => &e.event_time,
            UpdateScheduleEvent::ScheduleResumed(e) => &e.event_time,
            UpdateScheduleEvent::ScheduleModified(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(UpdateScheduleEvent);
impl_enum_variant!(UpdateScheduleEvent::ScheduleCreated(UpdateScheduleCreated));
impl_enum_variant!(UpdateScheduleEvent::SchedulePaused(UpdateSchedulePaused));
impl_enum_variant!(UpdateScheduleEvent::ScheduleResumed(UpdateScheduleResumed));
impl_enum_variant!(UpdateScheduleEvent::ScheduleModified(
    UpdateScheduleModified
));

/////////////////////////////////////////////////////////////////////////////////////////
