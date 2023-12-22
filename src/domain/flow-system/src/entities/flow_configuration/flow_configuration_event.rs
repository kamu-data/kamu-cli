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

use crate::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowConfigurationEvent {
    Created(FlowConfigurationEventCreated),
    Modified(FlowConfigurationEventModified),
    DatasetRemoved(FlowConfigurationEventDatasetRemoved),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventCreated {
    pub event_time: DateTime<Utc>,
    pub flow_key: FlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventModified {
    pub event_time: DateTime<Utc>,
    pub flow_key: FlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventDatasetRemoved {
    pub event_time: DateTime<Utc>,
    pub flow_key: FlowKey,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl FlowConfigurationEvent {
    pub fn flow_key(&self) -> &FlowKey {
        match self {
            FlowConfigurationEvent::Created(e) => &e.flow_key,
            FlowConfigurationEvent::Modified(e) => &e.flow_key,
            FlowConfigurationEvent::DatasetRemoved(e) => &e.flow_key,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            FlowConfigurationEvent::Created(e) => &e.event_time,
            FlowConfigurationEvent::Modified(e) => &e.event_time,
            FlowConfigurationEvent::DatasetRemoved(e) => &e.event_time,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(FlowConfigurationEvent);
impl_enum_variant!(FlowConfigurationEvent::Created(
    FlowConfigurationEventCreated
));
impl_enum_variant!(FlowConfigurationEvent::Modified(
    FlowConfigurationEventModified
));
impl_enum_variant!(FlowConfigurationEvent::DatasetRemoved(
    FlowConfigurationEventDatasetRemoved
));

/////////////////////////////////////////////////////////////////////////////////////////
