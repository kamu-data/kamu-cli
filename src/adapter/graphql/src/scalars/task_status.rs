// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system as ts;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Life-cycle status of a task
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting for capacity to be allocated to it
    Queued,
    /// Task is being executed
    Running,
    /// Task has reached a certain final outcome (see [`TaskOutcome`])
    Finished,
}

impl From<&ts::TaskStatus> for TaskStatus {
    fn from(v: &ts::TaskStatus) -> Self {
        match v {
            ts::TaskStatus::Queued => Self::Queued,
            ts::TaskStatus::Running => Self::Running,
            ts::TaskStatus::Finished => Self::Finished,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
