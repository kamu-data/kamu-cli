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

/// Health and timing of a recurring flow process
#[derive(SimpleObject, PartialEq, Eq)]
pub struct FlowProcessSummary {
    /// Current state of the process
    pub effective_state: FlowProcessEffectiveState,
    /// Number of failed runs in a row, reset by a success
    pub consecutive_failures: u32,
    /// When the trigger stops itself after failures
    pub stop_policy: FlowTriggerStopPolicy,
    /// Completion time of the last successful run, null if none succeeded
    pub last_success_at: Option<DateTime<Utc>>,
    /// Completion time of the last run, successful or failed
    pub last_attempt_at: Option<DateTime<Utc>>,
    /// Completion time of the last failed run, null if none failed
    pub last_failure_at: Option<DateTime<Utc>>,
    /// Planned time of the next run, null if none is planned
    pub next_planned_at: Option<DateTime<Utc>>,
    /// Start time of the run in progress, null if none is running
    pub running_since: Option<DateTime<Utc>>,
    /// Time the user paused the process, null if not paused
    pub paused_at: Option<DateTime<Utc>>,
    /// Why the process was stopped automatically, null if it was not
    pub auto_stopped_reason: Option<FlowProcessAutoStopReason>,
    /// Time the process was stopped automatically, null if it was not
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

/// Overall state of a flow process
#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowProcessEffectiveState")]
pub enum FlowProcessEffectiveState {
    /// No trigger is set, the flow runs only manually
    Unconfigured,
    /// Trigger is enabled and the last run did not fail
    Active,
    /// Trigger is enabled but recent runs failed
    Failing,
    /// Trigger was paused by a user
    PausedManual,
    /// Trigger was stopped automatically after failures
    StoppedAuto,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why a flow process was stopped automatically
#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
#[graphql(remote = "kamu_flow_system::FlowProcessAutoStopReason")]
pub enum FlowProcessAutoStopReason {
    /// Failures in a row reached the stop policy threshold
    StopPolicy,
    /// A run failed in a way that retrying cannot fix
    UnrecoverableFailure,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
