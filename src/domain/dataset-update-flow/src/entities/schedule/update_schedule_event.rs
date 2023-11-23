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
    Created(UpdateScheduleEventCreated),
    Modified(UpdateScheduleEventModified),
    DatasetRemoved(UpdateScheduleEventDatasetRemoved),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleEventCreated {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub schedule: Schedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleEventModified {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub paused: bool,
    pub schedule: Schedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScheduleEventDatasetRemoved {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl UpdateScheduleEvent {
    pub fn dataset_id(&self) -> &DatasetID {
        match self {
            UpdateScheduleEvent::Created(e) => &e.dataset_id,
            UpdateScheduleEvent::Modified(e) => &e.dataset_id,
            UpdateScheduleEvent::DatasetRemoved(e) => &e.dataset_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            UpdateScheduleEvent::Created(e) => &e.event_time,
            UpdateScheduleEvent::Modified(e) => &e.event_time,
            UpdateScheduleEvent::DatasetRemoved(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(UpdateScheduleEvent);
impl_enum_variant!(UpdateScheduleEvent::Created(UpdateScheduleEventCreated));
impl_enum_variant!(UpdateScheduleEvent::Modified(UpdateScheduleEventModified));
impl_enum_variant!(UpdateScheduleEvent::DatasetRemoved(
    UpdateScheduleEventDatasetRemoved
));

/////////////////////////////////////////////////////////////////////////////////////////
