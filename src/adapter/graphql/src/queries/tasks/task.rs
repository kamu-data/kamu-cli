// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use chrono::{DateTime, Utc};
use kamu_task_system as ts;

use crate::scalars::*;

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct Task {
    state: ts::TaskState,
}

#[Object]
impl Task {
    #[graphql(skip)]
    pub fn new(state: ts::TaskState) -> Self {
        Self { state }
    }

    /// Unique and stable identitfier of this task
    pub async fn task_id(&self) -> TaskID {
        self.state.task_id.into()
    }

    /// Life-cycle status of a task
    pub async fn status(&self) -> TaskStatus {
        self.state.status.into()
    }

    /// Whether the task was ordered to be cancelled
    pub async fn cancellation_requested(&self) -> bool {
        self.state.cancellation_requested
    }

    /// Describes a certain final outcome of the task once it reaches the
    /// "finished" status
    pub async fn outcome(&self) -> Option<TaskOutcome> {
        match &self.state.status {
            ts::TaskStatus::Queued | ts::TaskStatus::Running => None,
            ts::TaskStatus::Finished(outcome) => Some((*outcome).into()),
        }
    }

    /// Time when task was originally created and placed in a queue
    pub async fn created_at(&self) -> DateTime<Utc> {
        self.state.created_at.clone()
    }

    /// Time when task transitioned into a running state
    pub async fn ran_at(&self) -> Option<DateTime<Utc>> {
        self.state.ran_at.clone()
    }

    /// Time when cancellation of task was requested
    pub async fn cancellation_requested_at(&self) -> Option<DateTime<Utc>> {
        self.state.cancellation_requested_at.clone()
    }

    /// Time when task has reached a final outcome
    pub async fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.state.finished_at.clone()
    }
}
