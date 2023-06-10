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
use crate::repos::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct Task {
    state: TaskState,
    pending_events: Vec<TaskEvent>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl Task {
    pub fn new(event_store: &dyn TaskEventStore, logical_plan: LogicalPlan) -> Self {
        let task_id = event_store.new_task_id();

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

    pub fn status(&self) -> &TaskStatus {
        &self.state.status
    }

    pub fn logical_plan(&self) -> &LogicalPlan {
        &self.state.logical_plan
    }

    pub fn set_running(&mut self) -> Result<(), IllegalSequenceError<TaskState, TaskEvent>> {
        self.apply(
            TaskRunning {
                task_id: *self.id(),
            }
            .into(),
        )
    }

    pub fn finish(
        &mut self,
        outcome: TaskOutcome,
    ) -> Result<(), IllegalSequenceError<TaskState, TaskEvent>> {
        self.apply(
            TaskFinished {
                task_id: *self.id(),
                outcome,
            }
            .into(),
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Aggregate for Task {
    type Id = TaskID;
    type EventType = TaskEvent;
    type ProjectionType = TaskState;
    type EventStore = dyn TaskEventStore;

    fn from_state(state: Self::ProjectionType) -> Self {
        Self {
            state,
            pending_events: Vec::new(),
        }
    }

    fn from_genesis_event(
        event: Self::EventType,
    ) -> Result<Self, ProjectionError<Self::ProjectionType, Self::EventType>> {
        if !event.is_variant::<TaskCreated>() {
            return Err(IllegalGenesisError { event }.into());
        }

        let TaskCreated {
            task_id,
            logical_plan,
        } = event.into_variant().unwrap();

        Ok(Self::from_state(TaskState {
            task_id,
            status: TaskStatus::Queued,
            cancellation_requested: false,
            logical_plan,
        }))
    }

    async fn load(
        id: &Self::Id,
        event_store: &Self::EventStore,
    ) -> Result<Option<Self>, LoadError<Self::ProjectionType, Self::EventType>> {
        let event_stream = event_store.get_events_by_task(id, None, None);
        Self::from_event_stream(event_stream).await
    }

    // TODO: Concurrency control
    async fn save(&mut self, event_store: &Self::EventStore) -> Result<(), SaveError> {
        if !self.pending_events.is_empty() {
            let pending_events = std::mem::take(&mut self.pending_events);
            event_store.save_events(pending_events).await?;
        }
        Ok(())
    }

    fn apply(
        &mut self,
        event: Self::EventType,
    ) -> Result<(), IllegalSequenceError<Self::ProjectionType, Self::EventType>> {
        self.apply_no_save(event.clone())?;
        self.pending_events.push(event);
        Ok(())
    }

    fn apply_no_save(
        &mut self,
        event: Self::EventType,
    ) -> Result<(), IllegalSequenceError<Self::ProjectionType, Self::EventType>> {
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

    fn id(&self) -> &Self::Id {
        &self.state.task_id
    }

    fn as_state(&self) -> &Self::ProjectionType {
        &self.state
    }

    fn into_state(self) -> Self::ProjectionType {
        assert!(self.pending_events.is_empty());
        self.state
    }
}
