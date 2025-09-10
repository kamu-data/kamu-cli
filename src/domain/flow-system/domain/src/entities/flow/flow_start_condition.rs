// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Duration, Utc};
use kamu_task_system::TaskID;
use serde::{Deserialize, Serialize};

use crate::ReactiveRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowStartCondition {
    Schedule(FlowStartConditionSchedule),
    Throttling(FlowStartConditionThrottling),
    Reactive(FlowStartConditionReactive),
    Executor(FlowStartConditionExecutor),
}

impl FlowStartCondition {
    pub fn wake_up_at(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::Schedule(s) => Some(s.wake_up_at),
            Self::Throttling(t) => Some(t.wake_up_at),
            Self::Reactive(_) | Self::Executor(_) => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStartConditionSchedule {
    pub wake_up_at: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[serde_with::serde_as]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStartConditionThrottling {
    #[serde_as(as = "serde_with::DurationMilliSeconds<i64>")]
    pub interval: Duration,
    pub wake_up_at: DateTime<Utc>,
    pub shifted_from: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStartConditionReactive {
    pub active_rule: ReactiveRule,
    pub batching_deadline: DateTime<Utc>,
    #[serde(default)] // for backward compatibility only
    pub last_activation_cause_index: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowStartConditionExecutor {
    pub task_id: TaskID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
