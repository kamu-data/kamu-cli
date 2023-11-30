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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpdateConfigurationEvent {
    Created(UpdateConfigurationEventCreated),
    Modified(UpdateConfigurationEventModified),
    DatasetRemoved(UpdateConfigurationEventDatasetRemoved),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateConfigurationEventCreated {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub paused: bool,
    pub rule: UpdateConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateConfigurationEventModified {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub paused: bool,
    pub rule: UpdateConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateConfigurationEventDatasetRemoved {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl UpdateConfigurationEvent {
    pub fn dataset_id(&self) -> &DatasetID {
        match self {
            UpdateConfigurationEvent::Created(e) => &e.dataset_id,
            UpdateConfigurationEvent::Modified(e) => &e.dataset_id,
            UpdateConfigurationEvent::DatasetRemoved(e) => &e.dataset_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            UpdateConfigurationEvent::Created(e) => &e.event_time,
            UpdateConfigurationEvent::Modified(e) => &e.event_time,
            UpdateConfigurationEvent::DatasetRemoved(e) => &e.event_time,
        }
    }
}

impl_enum_with_variants!(UpdateConfigurationEvent);
impl_enum_variant!(UpdateConfigurationEvent::Created(
    UpdateConfigurationEventCreated
));
impl_enum_variant!(UpdateConfigurationEvent::Modified(
    UpdateConfigurationEventModified
));
impl_enum_variant!(UpdateConfigurationEvent::DatasetRemoved(
    UpdateConfigurationEventDatasetRemoved
));

/////////////////////////////////////////////////////////////////////////////////////////
