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
pub enum FlowConfigurationEvent<TFlowKey> {
    Created(FlowConfigurationEventCreated<TFlowKey>),
    Modified(FlowConfigurationEventModified<TFlowKey>),
    DatasetRemoved(FlowConfigurationEventDatasetRemoved<TFlowKey>),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventCreated<TFlowKey> {
    pub event_time: DateTime<Utc>,
    pub flow_key: TFlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventModified<TFlowKey> {
    pub event_time: DateTime<Utc>,
    pub flow_key: TFlowKey,
    pub paused: bool,
    pub rule: FlowConfigurationRule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowConfigurationEventDatasetRemoved<TFlowKey> {
    pub event_time: DateTime<Utc>,
    pub flow_key: TFlowKey,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<TFlowKey> FlowConfigurationEvent<TFlowKey> {
    pub fn flow_key(&self) -> &TFlowKey {
        match self {
            FlowConfigurationEvent::<TFlowKey>::Created(e) => &e.flow_key,
            FlowConfigurationEvent::<TFlowKey>::Modified(e) => &e.flow_key,
            FlowConfigurationEvent::<TFlowKey>::DatasetRemoved(e) => &e.flow_key,
        }
    }

    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            FlowConfigurationEvent::<TFlowKey>::Created(e) => &e.event_time,
            FlowConfigurationEvent::<TFlowKey>::Modified(e) => &e.event_time,
            FlowConfigurationEvent::<TFlowKey>::DatasetRemoved(e) => &e.event_time,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl_generic_enum_with_variants!(FlowConfigurationEvent<TFlowKey: Clone>);
impl_generic_enum_variant!(
    FlowConfigurationEvent::Created(FlowConfigurationEventCreated<TFlowKey: Clone>)
);
impl_generic_enum_variant!(
    FlowConfigurationEvent::Modified(FlowConfigurationEventModified<TFlowKey: Clone>)
);
impl_generic_enum_variant!(
    FlowConfigurationEvent::DatasetRemoved(FlowConfigurationEventDatasetRemoved<TFlowKey: Clone>)
);

/////////////////////////////////////////////////////////////////////////////////////////
