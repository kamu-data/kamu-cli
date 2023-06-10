// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use enum_variants::*;

use super::errors::*;
use super::Aggregate;
use crate::entities::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Task {
    state: TaskState,
    pending_events: Vec<TaskEvent>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Task {
    pub fn new(task_id: TaskID, logical_plan: LogicalPlan) -> Self {
        let genesis = TaskCreated {
            task_id,
            logical_plan,
        };

        Self {
            state: TaskState {
                task_id,
                status: TaskStatus::Queued,
                cancellation_requested: false,
                logical_plan: genesis.logical_plan.clone(),
            },
            pending_events: vec![genesis.into()],
        }
    }

    /// Transition task to a `Running` state
    pub fn run(&mut self) -> Result<(), IllegalSequenceError<Self>> {
        self.apply(TaskRunning {
            task_id: self.task_id,
        })
    }

    /// Set cancellation flag (if not already set)
    pub fn cancel(&mut self) -> Result<(), IllegalSequenceError<Self>> {
        if self.cancellation_requested {
            return Ok(());
        }

        self.apply(TaskCancelled {
            task_id: self.task_id,
        })
    }

    /// Transition task to a `Finished` state with the specified outcome
    pub fn finish(&mut self, outcome: TaskOutcome) -> Result<(), IllegalSequenceError<Self>> {
        self.apply(TaskFinished {
            task_id: self.task_id,
            outcome,
        })
    }

    fn apply(&mut self, event: impl Into<TaskEvent>) -> Result<(), IllegalSequenceError<Self>> {
        let event = event.into();
        self.mutate(event.clone())?;
        self.pending_events.push(event);
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Aggregate for Task {
    type Id = TaskID;
    type Event = TaskEvent;
    type State = TaskState;

    fn from_genesis_event(event: TaskEvent) -> Result<Self, IllegalGenesisError<Self>> {
        if !event.is_variant::<TaskCreated>() {
            return Err(IllegalGenesisError { event });
        }

        let TaskCreated {
            task_id,
            logical_plan,
        } = event.into_variant().unwrap();

        Ok(Self::from_snapshot(TaskState {
            task_id,
            status: TaskStatus::Queued,
            cancellation_requested: false,
            logical_plan,
        }))
    }

    fn from_snapshot(state: TaskState) -> Self {
        Self {
            state,
            pending_events: Vec::new(),
        }
    }

    fn mutate(&mut self, event: TaskEvent) -> Result<(), IllegalSequenceError<Self>> {
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

    fn updates(&mut self) -> Vec<TaskEvent> {
        // Extra check to avoid taking a vec with an allocated buffer
        if self.pending_events.is_empty() {
            Vec::new()
        } else {
            std::mem::take(&mut self.pending_events)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl std::ops::Deref for Task {
    type Target = TaskState;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl AsRef<TaskState> for Task {
    fn as_ref(&self) -> &TaskState {
        &self.state
    }
}

impl Into<TaskState> for Task {
    fn into(self) -> TaskState {
        assert!(self.pending_events.is_empty());
        self.state
    }
}
