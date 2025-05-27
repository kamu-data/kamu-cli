// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use event_sourcing::*;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Aggregate, Debug)]
pub struct Task(Aggregate<TaskState, (dyn TaskEventStore + 'static)>);

impl Task {
    /// Creates a task with a pending `TaskCreated` event
    pub fn new(
        now: DateTime<Utc>,
        task_id: TaskID,
        logical_plan: LogicalPlan,
        metadata: Option<TaskMetadata>,
    ) -> Self {
        Self(
            Aggregate::new(
                task_id,
                TaskEventCreated {
                    event_time: now,
                    task_id,
                    logical_plan,
                    metadata,
                },
            )
            .unwrap(),
        )
    }

    /// Transition task to a `Running` state
    pub fn run(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<TaskState>> {
        let event = TaskEventRunning {
            event_time: now,
            task_id: self.task_id,
        };
        self.apply(event)
    }

    /// Task is queued or running and cancellation was not already requested
    pub fn can_cancel(&self) -> bool {
        matches!(self.status(), TaskStatus::Queued | TaskStatus::Running if self.timing.cancellation_requested_at.is_none())
    }

    /// Set cancellation flag (if not already set)
    pub fn cancel(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<TaskState>> {
        if self.timing.cancellation_requested_at.is_some() {
            return Ok(());
        }

        let event = TaskEventCancelled {
            event_time: now,
            task_id: self.task_id,
        };
        self.apply(event)
    }

    /// Transition task to a `Finished` state with the specified outcome
    pub fn finish(
        &mut self,
        now: DateTime<Utc>,
        outcome: TaskOutcome,
    ) -> Result<(), ProjectionError<TaskState>> {
        let event = TaskEventFinished {
            event_time: now,
            task_id: self.task_id,
            outcome,
        };
        self.apply(event)
    }

    pub fn requeue(&mut self, now: DateTime<Utc>) -> Result<(), ProjectionError<TaskState>> {
        let event = TaskEventRequeued {
            event_time: now,
            task_id: self.task_id,
        };
        self.apply(event)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
