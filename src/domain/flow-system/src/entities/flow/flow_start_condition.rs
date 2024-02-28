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

use crate::BatchingRule;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowStartCondition {
    Throttling(FlowStartConditionThrottling),
    Batching(FlowStartConditionBatching),
    Executor(FlowStartConditionExecutor),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowStartConditionThrottling {
    pub interval: Duration,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowStartConditionBatching {
    pub active_batching_rule: BatchingRule,
    pub batching_deadline: DateTime<Utc>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowStartConditionExecutor {
    pub task_id: TaskID,
}

/////////////////////////////////////////////////////////////////////////////////////////
