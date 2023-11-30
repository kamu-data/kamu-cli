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
pub enum DatasetFlowConfigurationEvent {
    Created(DatasetFlowConfigurationEventCreated),
    Modified(DatasetFlowConfigurationEventModified),
    DatasetRemoved(DatasetFlowConfigurationEventDatasetRemoved),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowConfigurationEventCreated {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub paused: bool,
    pub rule: DatasetFlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowConfigurationEventModified {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
    pub paused: bool,
    pub rule: DatasetFlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatasetFlowConfigurationEventDatasetRemoved {
    pub event_time: DateTime<Utc>,
    pub dataset_id: DatasetID,
    pub flow_type: DatasetFlowType,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetFlowConfigurationEvent {
    pub fn dataset_id(&self) -> &DatasetID {
        match self {
            DatasetFlowConfigurationEvent::Created(e) => &e.dataset_id,
            DatasetFlowConfigurationEvent::Modified(e) => &e.dataset_id,
            DatasetFlowConfigurationEvent::DatasetRemoved(e) => &e.dataset_id,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            DatasetFlowConfigurationEvent::Created(e) => &e.event_time,
            DatasetFlowConfigurationEvent::Modified(e) => &e.event_time,
            DatasetFlowConfigurationEvent::DatasetRemoved(e) => &e.event_time,
        }
    }

    pub fn flow_type(&self) -> DatasetFlowType {
        match self {
            DatasetFlowConfigurationEvent::Created(e) => e.flow_type,
            DatasetFlowConfigurationEvent::Modified(e) => e.flow_type,
            DatasetFlowConfigurationEvent::DatasetRemoved(e) => e.flow_type,
        }
    }
}

impl_enum_with_variants!(DatasetFlowConfigurationEvent);
impl_enum_variant!(DatasetFlowConfigurationEvent::Created(
    DatasetFlowConfigurationEventCreated
));
impl_enum_variant!(DatasetFlowConfigurationEvent::Modified(
    DatasetFlowConfigurationEventModified
));
impl_enum_variant!(DatasetFlowConfigurationEvent::DatasetRemoved(
    DatasetFlowConfigurationEventDatasetRemoved
));

/////////////////////////////////////////////////////////////////////////////////////////
