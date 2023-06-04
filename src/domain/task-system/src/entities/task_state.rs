// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone)]
pub struct TaskState {
    pub task_id: TaskID,
    pub status: TaskStatus,
    pub logical_plan: LogicalPlan,
}

/////////////////////////////////////////////////////////////////////////////////////////
