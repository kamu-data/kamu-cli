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
use serde::{Deserialize, Serialize};

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowConfigurationEvent {
    Created(FlowConfigurationEventCreated),
    Modified(FlowConfigurationEventModified),
    ScopeRemoved(FlowConfigurationEventScopeRemoved),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigurationEventCreated {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
    pub rule: FlowConfigurationRule,
    pub retry_policy: Option<RetryPolicy>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigurationEventModified {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
    pub rule: FlowConfigurationRule,
    pub retry_policy: Option<RetryPolicy>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowConfigurationEventScopeRemoved {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowConfigurationEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            FlowConfigurationEvent::Created(_) => "FlowConfigurationEventCreated",
            FlowConfigurationEvent::Modified(_) => "FlowConfigurationEventModified",
            FlowConfigurationEvent::ScopeRemoved(_) => "FlowConfigurationEventScopeRemoved",
        }
    }

    pub fn flow_binding(&self) -> &FlowBinding {
        match self {
            FlowConfigurationEvent::Created(e) => &e.flow_binding,
            FlowConfigurationEvent::Modified(e) => &e.flow_binding,
            FlowConfigurationEvent::ScopeRemoved(e) => &e.flow_binding,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            FlowConfigurationEvent::Created(e) => e.event_time,
            FlowConfigurationEvent::Modified(e) => e.event_time,
            FlowConfigurationEvent::ScopeRemoved(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(FlowConfigurationEvent);
impl_enum_variant!(FlowConfigurationEvent::Created(
    FlowConfigurationEventCreated
));
impl_enum_variant!(FlowConfigurationEvent::Modified(
    FlowConfigurationEventModified
));
impl_enum_variant!(FlowConfigurationEvent::ScopeRemoved(
    FlowConfigurationEventScopeRemoved
));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
