// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dill::*;
use kamu_core::SystemTimeSource;
use kamu_task_system::*;
use opendatafabric::DatasetID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskSchedulerImpl {
    state: Arc<Mutex<State>>,
    // TODO: EventStore is transaction-dependent, it can't be instantiated in a singleton
    event_store: Arc<dyn TaskSystemEventStore>,
    time_source: Arc<dyn SystemTimeSource>,
}

#[derive(Default)]
struct State {
    // TODO: store in DB or something like Redis
    task_queue: VecDeque<TaskID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskScheduler)]
#[scope(Singleton)]
impl TaskSchedulerImpl {
    pub fn new(
        event_store: Arc<dyn TaskSystemEventStore>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            event_store,
            time_source,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskScheduler for TaskSchedulerImpl {
    #[tracing::instrument(level = "info", skip_all, fields(?logical_plan))]
    async fn create_task(&self, logical_plan: LogicalPlan) -> Result<TaskState, CreateTaskError> {
        let mut task = Task::new(
            self.time_source.now(),
            self.event_store.new_task_id().await?,
            logical_plan,
        );
        task.save(self.event_store.as_ref()).await.int_err()?;

        let queue_len = {
            let mut state = self.state.lock().unwrap();
            state.task_queue.push_back(task.task_id);
            state.task_queue.len()
        };

        tracing::info!(
            task_id = %task.task_id,
            queue_len,
            "Task queued"
        );

        Ok(task.into())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn get_task(&self, task_id: TaskID) -> Result<TaskState, GetTaskError> {
        let task = Task::load(task_id, self.event_store.as_ref()).await?;
        Ok(task.into())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%task_id))]
    async fn cancel_task(&self, task_id: TaskID) -> Result<TaskState, CancelTaskError> {
        let mut task = Task::load(task_id, self.event_store.as_ref()).await?;

        if task.can_cancel() {
            task.cancel(self.time_source.now()).int_err()?;
            task.save(self.event_store.as_ref()).await.int_err()?;

            let mut state = self.state.lock().unwrap();
            state.task_queue.retain(|task_id| *task_id != task.task_id);
        }

        Ok(task.into())
    }

    #[tracing::instrument(level = "info", skip_all, fields(%dataset_id))]
    async fn list_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
        pagination: TaskPaginationOpts,
    ) -> Result<TaskStateListing, ListTasksByDatasetError> {
        let total_count = self
            .event_store
            .get_count_tasks_by_dataset(dataset_id)
            .await?;

        let dataset_id = dataset_id.clone();

        use futures::TryStreamExt;
        let stream = Box::pin(async_stream::stream! {
            let relevant_task_ids: Vec<_> = self
                .event_store
                .get_tasks_by_dataset(&dataset_id, pagination)
                .await
                .try_collect()
                .await
                .int_err()?;

            // TODO: implement batch loading
            for task_id in relevant_task_ids {
                let task = Task::load(task_id, self.event_store.as_ref()).await.int_err()?;
                yield Ok(task.into());
            }
        });

        Ok(TaskStateListing {
            stream,
            total_count,
        })
    }

    // TODO: Use signaling instead of a loop
    async fn take(&self) -> Result<TaskID, TakeTaskError> {
        loop {
            if let Some(task_id) = self.try_take().await? {
                return Ok(task_id);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    // TODO: How to prevent tasks from being lost if executor crashes
    async fn try_take(&self) -> Result<Option<TaskID>, TakeTaskError> {
        let task_id = {
            let mut s = self.state.lock().unwrap();
            s.task_queue.pop_front()
        };

        let Some(task_id) = task_id else {
            return Ok(None);
        };

        let mut task = Task::load(task_id, self.event_store.as_ref())
            .await
            .int_err()?;
        task.run(self.time_source.now()).int_err()?;
        task.save(self.event_store.as_ref()).await.int_err()?;

        tracing::info!(
            %task_id,
            logical_plan = ?task.logical_plan,
            "Handing over a task to an executor",
        );

        Ok(Some(task_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
