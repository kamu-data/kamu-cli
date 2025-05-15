// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_task_system::*;
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskSchedulerImpl {
    task_event_store: Arc<dyn TaskEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskScheduler)]
impl TaskSchedulerImpl {
    pub fn new(
        task_event_store: Arc<dyn TaskEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            task_event_store,
            time_source,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskScheduler for TaskSchedulerImpl {
    async fn create_task(
        &self,
        logical_plan: LogicalPlan,
        metadata: Option<TaskMetadata>,
    ) -> Result<TaskState, CreateTaskError> {
        tracing::info!(logical_plan = ?logical_plan, "Creating task");

        let mut task = Task::new(
            self.time_source.now(),
            self.task_event_store.new_task_id().await?,
            logical_plan,
            metadata,
        );
        task.save(self.task_event_store.as_ref()).await.int_err()?;

        tracing::info!(
            task_id = %task.task_id,
            "Task queued"
        );

        Ok(task.into())
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%task_id))]
    async fn get_task(&self, task_id: TaskID) -> Result<TaskState, GetTaskError> {
        let task = Task::load(task_id, self.task_event_store.as_ref()).await?;
        Ok(task.into())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn cancel_task(&self, task_id: TaskID) -> Result<TaskState, CancelTaskError> {
        let mut task = Task::load(task_id, self.task_event_store.as_ref()).await?;

        if task.can_cancel() {
            task.cancel(self.time_source.now()).int_err()?;
            task.save(self.task_event_store.as_ref()).await.int_err()?;
        }

        Ok(task.into())
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn try_take(&self) -> Result<Option<Task>, TakeTaskError> {
        // Try reading just 1 earliest queued task
        let Some(task_id) = self.task_event_store.try_get_queued_task().await? else {
            // No queued tasks yet..
            return Ok(None);
        };

        // Mark the task as running and hand it over to Agent
        let mut task = Task::load(task_id, self.task_event_store.as_ref())
            .await
            .int_err()?;
        task.run(self.time_source.now()).int_err()?;
        task.save(self.task_event_store.as_ref()).await.int_err()?;

        tracing::info!(
            %task_id,
            logical_plan = ?task.logical_plan,
            "Handing over a task to an agent",
        );

        Ok(Some(task))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
