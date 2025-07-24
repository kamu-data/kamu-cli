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
pub enum FlowTriggerEvent {
    Created(FlowTriggerEventCreated),
    Modified(FlowTriggerEventModified),
    ScopeRemoved(FlowTriggerEventScopeRemoved),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTriggerEventCreated {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
    pub paused: bool,
    pub rule: FlowTriggerRule,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTriggerEventModified {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
    pub paused: bool,
    pub rule: FlowTriggerRule,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowTriggerEventScopeRemoved {
    pub event_time: DateTime<Utc>,
    pub flow_binding: FlowBinding,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowTriggerEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            Self::Created(_) => "FlowTriggerEventCreated",
            Self::Modified(_) => "FlowTriggerEventModified",
            Self::ScopeRemoved(_) => "FlowTriggerEventScopeRemoved",
        }
    }

    pub fn flow_binding(&self) -> &FlowBinding {
        match self {
            Self::Created(e) => &e.flow_binding,
            Self::Modified(e) => &e.flow_binding,
            Self::ScopeRemoved(e) => &e.flow_binding,
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::Created(e) => e.event_time,
            Self::Modified(e) => e.event_time,
            Self::ScopeRemoved(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(FlowTriggerEvent);
impl_enum_variant!(FlowTriggerEvent::Created(FlowTriggerEventCreated));
impl_enum_variant!(FlowTriggerEvent::Modified(FlowTriggerEventModified));
impl_enum_variant!(FlowTriggerEvent::ScopeRemoved(FlowTriggerEventScopeRemoved));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
