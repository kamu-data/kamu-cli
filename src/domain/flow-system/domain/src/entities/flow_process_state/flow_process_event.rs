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
pub enum FlowProcessEvent {
    EffectiveStateChanged(FlowProcessEventEffectiveStateChanged),
    AutoStopped(FlowProcessEventAutoStopped),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProcessEventEffectiveStateChanged {
    pub event_time: DateTime<Utc>,
    pub old_state: FlowProcessEffectiveState,
    pub new_state: FlowProcessEffectiveState,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProcessEventAutoStopped {
    pub event_time: DateTime<Utc>,
    pub reason: FlowProcessAutoStopReason,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowProcessEventResumedFromAutoStop {
    pub event_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessEvent {
    pub fn typename(&self) -> &'static str {
        match self {
            Self::EffectiveStateChanged(_) => "FlowProcessEventEffectiveStateChanged",
            Self::AutoStopped(_) => "FlowProcessEventAutoStopped",
        }
    }

    pub fn event_time(&self) -> DateTime<Utc> {
        match self {
            Self::EffectiveStateChanged(e) => e.event_time,
            Self::AutoStopped(e) => e.event_time,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl_enum_with_variants!(FlowProcessEvent);
impl_enum_variant!(FlowProcessEvent::EffectiveStateChanged(
    FlowProcessEventEffectiveStateChanged
));
impl_enum_variant!(FlowProcessEvent::AutoStopped(FlowProcessEventAutoStopped));

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
