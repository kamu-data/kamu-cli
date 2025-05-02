// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_task_system as ts;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Task {
    state: ts::TaskState,
}

#[Object]
impl Task {
    #[graphql(skip)]
    pub fn new(state: ts::TaskState) -> Self {
        Self { state }
    }

    /// Unique and stable identifier of this task
    async fn task_id(&self) -> TaskID {
        self.state.task_id.into()
    }

    /// Life-cycle status of a task
    async fn status(&self) -> TaskStatus {
        (&self.state.status()).into()
    }

    /// Whether the task was ordered to be cancelled
    async fn cancellation_requested(&self) -> bool {
        self.state.timing.cancellation_requested_at.is_some()
    }

    /// Describes a certain final outcome of the task once it reaches the
    /// "finished" status
    async fn outcome(&self) -> Option<TaskOutcome> {
        self.state.outcome().map(Into::into)
    }

    /// Time when task was originally created and placed in a queue
    async fn created_at(&self) -> DateTime<Utc> {
        self.state.timing.created_at
    }

    /// Time when task transitioned into a running state
    async fn ran_at(&self) -> Option<DateTime<Utc>> {
        self.state.ran_at()
    }

    /// Time when cancellation of task was requested
    async fn cancellation_requested_at(&self) -> Option<DateTime<Utc>> {
        self.state.timing.cancellation_requested_at
    }

    /// Time when task has reached a final outcome
    async fn finished_at(&self) -> Option<DateTime<Utc>> {
        self.state.finished_at()
    }
}
