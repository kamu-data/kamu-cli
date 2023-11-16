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
pub enum DatasetUpdateFlowEvent {
    /// New update flow created
    FlowCreated(DatasetUpdateFlowCreated),
    /// Flow paused
    FlowPaused(DatasetUpdateFlowPaused),
    /// Flow resumed
    FlowResumed(DatasetUpdateFlowResumed),
    /// Schedule modified
    ScheduleModified(DatasetUpdateFlowScheduleModified),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdateFlowCreated {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub schedule: UpdateSchedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdateFlowPaused {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdateFlowResumed {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetUpdateFlowScheduleModified {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub new_schedule: UpdateSchedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetUpdateFlowEvent {
    pub fn dataset_id(&self) -> &DatasetID {
        match self {
            DatasetUpdateFlowEvent::FlowCreated(e) => &e.dataset_id,
            DatasetUpdateFlowEvent::FlowPaused(e) => &e.dataset_id,
            DatasetUpdateFlowEvent::FlowResumed(e) => &e.dataset_id,
            DatasetUpdateFlowEvent::ScheduleModified(e) => &e.dataset_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            DatasetUpdateFlowEvent::FlowCreated(e) => &e.event_time,
            DatasetUpdateFlowEvent::FlowPaused(e) => &e.event_time,
            DatasetUpdateFlowEvent::FlowResumed(e) => &e.event_time,
            DatasetUpdateFlowEvent::ScheduleModified(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(DatasetUpdateFlowEvent);
impl_enum_variant!(DatasetUpdateFlowEvent::FlowCreated(
    DatasetUpdateFlowCreated
));
impl_enum_variant!(DatasetUpdateFlowEvent::FlowPaused(DatasetUpdateFlowPaused));
impl_enum_variant!(DatasetUpdateFlowEvent::FlowResumed(
    DatasetUpdateFlowResumed
));
impl_enum_variant!(DatasetUpdateFlowEvent::ScheduleModified(
    DatasetUpdateFlowScheduleModified
));

/////////////////////////////////////////////////////////////////////////////////////////
