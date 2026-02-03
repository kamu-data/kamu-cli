// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowProcessSummary {
    pub effective_state: FlowProcessEffectiveState,
    pub consecutive_failures: u32,
    pub stop_policy: FlowTriggerStopPolicy,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub next_planned_at: Option<DateTime<Utc>>,
    pub running_since: Option<DateTime<Utc>>,
    pub paused_at: Option<DateTime<Utc>>,
    pub auto_stopped_reason: Option<FlowProcessAutoStopReason>,
    pub auto_stopped_at: Option<DateTime<Utc>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_flow_system::FlowProcessState> for FlowProcessSummary {
    fn from(value: kamu_flow_system::FlowProcessState) -> Self {
        Self {
            effective_state: value.effective_state().into(),
            consecutive_failures: value.consecutive_failures(),
            stop_policy: value.stop_policy().into(),
            last_success_at: value.last_success_at(),
            last_attempt_at: value.last_attempt_at(),
            last_failure_at: value.last_failure_at(),
            next_planned_at: value.next_planned_at(),
            running_since: value.running_since(),
            paused_at: value.paused_at(),
            auto_stopped_reason: value.auto_stopped_reason().map(Into::into),
            auto_stopped_at: value.auto_stopped_at(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowProcessEffectiveState")]
pub enum FlowProcessEffectiveState {
    Unconfigured,
    Active,
    Failing,
    PausedManual,
    StoppedAuto,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowProcessAutoStopReason")]
pub enum FlowProcessAutoStopReason {
    StopPolicy,
    UnrecoverableFailure,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
