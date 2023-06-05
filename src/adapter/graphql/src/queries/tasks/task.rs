// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_task_system as ts;

use crate::scalars::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Task {
    task_id: TaskID,
    status: TaskStatus,
    outcome: Option<TaskOutcome>,
}

#[Object]
impl Task {
    #[graphql(skip)]
    pub fn new(state: ts::TaskState) -> Self {
        // Unpack so that any update to domain model forces us to update this code
        let ts::TaskState {
            task_id,
            status,
            logical_plan: _,
        } = state;

        // Un-nest enum into a field
        let outcome = match &status {
            ts::TaskStatus::Queued | ts::TaskStatus::Running => None,
            ts::TaskStatus::Finished(outcome) => Some((*outcome).into()),
        };

        Self {
            task_id: task_id.into(),
            status: status.into(),
            outcome,
            //logical_plan: v.logical_plan.into(),
        }
    }

    /// Unique and stable identitfier of this task
    pub async fn id(&self) -> &TaskID {
        &self.task_id
    }

    /// Life-cycle status of a task
    pub async fn status(&self) -> TaskStatus {
        self.status
    }

    /// Describes a certain final outcome of the task once it reaches the
    /// "finished" status
    pub async fn outcome(&self) -> Option<TaskOutcome> {
        self.outcome
    }
}
