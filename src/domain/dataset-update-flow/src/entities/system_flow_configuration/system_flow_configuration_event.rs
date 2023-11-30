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
pub enum SystemFlowConfigurationEvent {
    Created(SystemFlowConfigurationEventCreated),
    Modified(SystemFlowConfigurationEventModified),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowConfigurationEventCreated {
    pub event_time: DateTime<Utc>,
    pub flow_type: SystemFlowType,
    pub paused: bool,
    pub schedule: Schedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemFlowConfigurationEventModified {
    pub event_time: DateTime<Utc>,
    pub flow_type: SystemFlowType,
    pub paused: bool,
    pub schedule: Schedule,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl SystemFlowConfigurationEvent {
    pub fn event_time(&self) -> &DateTime<Utc> {
        match self {
            SystemFlowConfigurationEvent::Created(e) => &e.event_time,
            SystemFlowConfigurationEvent::Modified(e) => &e.event_time,
        }
    }

    pub fn flow_type(&self) -> SystemFlowType {
        match self {
            SystemFlowConfigurationEvent::Created(e) => e.flow_type,
            SystemFlowConfigurationEvent::Modified(e) => e.flow_type,
        }
    }
}

impl_enum_with_variants!(SystemFlowConfigurationEvent);
impl_enum_variant!(SystemFlowConfigurationEvent::Created(
    SystemFlowConfigurationEventCreated
));
impl_enum_variant!(SystemFlowConfigurationEvent::Modified(
    SystemFlowConfigurationEventModified
));

/////////////////////////////////////////////////////////////////////////////////////////
