// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_variants::*;
use internal_error::*;

use super::*;

/////////////////////////////////////////////////////////////////////////////////////////

/// Represents the state of the task at specific point in time (projection)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskState {
    pub task_id: TaskID,
    pub status: TaskStatus,
    pub cancellation_requested: bool,
    pub logical_plan: LogicalPlan,
}

/////////////////////////////////////////////////////////////////////////////////////////

/// Projects a series of [TaskEvent]s into a [TaskState]
#[derive(Debug, Clone)]
pub struct TaskStateProjection {
    state: TaskState,
}

impl TaskStateProjection {
    pub fn new(event: TaskCreated) -> Self {
        let TaskCreated {
            task_id,
            logical_plan,
        } = event;

        Self {
            state: TaskState {
                task_id,
                status: TaskStatus::Queued,
                cancellation_requested: false,
                logical_plan,
            },
        }
    }

    pub fn try_from(event: TaskEvent) -> Result<Self, ProjectionError<TaskState, TaskEvent>> {
        if !event.is_variant::<TaskCreated>() {
            return Err(format!(
                "Expected first event to be TaskCreated but got: {:?}",
                event
            )
            .int_err()
            .into());
        }

        Ok(Self::new(event.into_variant().unwrap()))
    }

    pub fn project_iter(
        events: impl IntoIterator<Item = TaskEvent>,
    ) -> Result<Option<TaskState>, ProjectionError<TaskState, TaskEvent>> {
        let mut iter = events.into_iter();
        let first = match iter.next() {
            None => return Ok(None),
            Some(first) => first,
        };

        let mut proj = Self::try_from(first)?;
        iter.try_for_each(|e| proj.append(e))?;
        Ok(Some(proj.into_state()))
    }

    pub async fn project_stream(
        mut stream: impl tokio_stream::Stream<Item = Result<TaskEvent, InternalError>> + Unpin,
    ) -> Result<Option<TaskState>, ProjectionError<TaskState, TaskEvent>> {
        use tokio_stream::StreamExt;

        let first = match stream.next().await {
            None => return Ok(None),
            Some(first) => first?,
        };

        let mut proj = Self::try_from(first)?;

        while let Some(event) = stream.next().await {
            proj.append(event?)?;
        }

        Ok(Some(proj.into_state()))
    }

    pub fn append(
        &mut self,
        event: TaskEvent,
    ) -> Result<(), ProjectionError<TaskState, TaskEvent>> {
        assert_eq!(self.state.task_id, event.task_id());

        // Check if state transition is legal
        match (self.state.status, &event) {
            (TaskStatus::Queued, TaskEvent::Running(_)) => {}
            (TaskStatus::Queued | TaskStatus::Running, TaskEvent::Cancelled(_)) => {}
            (TaskStatus::Queued | TaskStatus::Running, TaskEvent::Finished(_)) => {}
            (_, _) => return Err(IllegalSequenceError::new(self.state.clone(), event).into()),
        }

        // Apply transition
        match event {
            TaskEvent::Created(_) => unreachable!(),
            TaskEvent::Running(TaskRunning { task_id: _ }) => {
                self.state.status = TaskStatus::Running;
            }
            TaskEvent::Cancelled(TaskCancelled { task_id: _ }) => {
                self.state.cancellation_requested = true;
            }
            TaskEvent::Finished(TaskFinished {
                task_id: _,
                outcome,
            }) => {
                self.state.status = TaskStatus::Finished(outcome);
            }
        }
        Ok(())
    }

    pub fn as_state(&self) -> &TaskState {
        &self.state
    }

    pub fn into_state(self) -> TaskState {
        self.state
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ProjectionError<Proj, Evt> {
    #[error(transparent)]
    IllegalSequence(#[from] IllegalSequenceError<Proj, Evt>),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(thiserror::Error, Debug)]
#[error("Event {event:?} is illegal for state {state:?}")]
pub struct IllegalSequenceError<Proj, Evt> {
    pub state: Proj,
    pub event: Evt,
}

impl<Proj, Evt> IllegalSequenceError<Proj, Evt> {
    pub fn new(state: Proj, event: Evt) -> Self {
        Self { state, event }
    }
}
