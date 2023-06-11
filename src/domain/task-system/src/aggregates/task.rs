// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use enum_variants::*;

use crate::entities::*;
use crate::es_common::*;

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
            event_time: Utc::now(),
            task_id,
            logical_plan,
        };

        let mut this = Self::from_genesis_event(genesis.clone().into()).unwrap();
        this.pending_events.push(genesis.into());
        this
    }

    /// Transition task to a `Running` state
    pub fn run(&mut self) -> Result<(), IllegalSequenceError<Self>> {
        self.apply(TaskRunning {
            event_time: Utc::now(),
            task_id: self.task_id,
        })
    }

    /// Set cancellation flag (if not already set)
    pub fn cancel(&mut self) -> Result<(), IllegalSequenceError<Self>> {
        if self.cancellation_requested {
            return Ok(());
        }

        self.apply(TaskCancelled {
            event_time: Utc::now(),
            task_id: self.task_id,
        })
    }

    /// Transition task to a `Finished` state with the specified outcome
    pub fn finish(&mut self, outcome: TaskOutcome) -> Result<(), IllegalSequenceError<Self>> {
        self.apply(TaskFinished {
            event_time: Utc::now(),
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
            event_time,
            task_id,
            logical_plan,
        } = event.into_variant().unwrap();

        Ok(Self::from_snapshot(TaskState {
            task_id,
            status: TaskStatus::Queued,
            cancellation_requested: false,
            logical_plan,
            created_at: event_time,
            ran_at: None,
            cancellation_requested_at: None,
            finished_at: None,
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
            TaskEvent::Running(TaskRunning {
                event_time,
                task_id: _,
            }) => {
                self.state.status = TaskStatus::Running;
                self.state.ran_at = Some(event_time);
            }
            TaskEvent::Cancelled(TaskCancelled {
                event_time,
                task_id: _,
            }) => {
                self.state.cancellation_requested = true;
                self.state.cancellation_requested_at = Some(event_time);
            }
            TaskEvent::Finished(TaskFinished {
                event_time,
                task_id: _,
                outcome,
            }) => {
                self.state.status = TaskStatus::Finished(outcome);
                self.state.finished_at = Some(event_time);
            }
        }
        Ok(())
    }

    fn has_updates(&self) -> bool {
        !self.pending_events.is_empty()
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
    fn into(mut self) -> TaskState {
        assert!(self.pending_events.is_empty());
        // Have to replace with dummy state because we implement Drop
        let task_id = self.state.task_id;
        let state = std::mem::replace(
            &mut self.state,
            TaskState {
                task_id,
                status: TaskStatus::Queued,
                cancellation_requested: false,
                logical_plan: LogicalPlan::Probe(Probe::default()),
                created_at: DateTime::<Utc>::MIN_UTC,
                ran_at: None,
                cancellation_requested_at: None,
                finished_at: None,
            },
        );
        state
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        if !self.pending_events.is_empty() {
            tracing::error!(
                task_id = %self.state.task_id,
                pending_events = ?self.pending_events,
                "Task is dropped with unsaved events",
            )
        }
    }
}
