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
use event_bus::EventBus;
use kamu_core::{PullOptions, PullService, SystemTimeSource};
use kamu_task_system::*;

pub struct TaskExecutorInMemory {
    task_sched: Arc<dyn TaskScheduler>,
    event_store: Arc<dyn TaskSystemEventStore>,
    event_bus: Arc<EventBus>,
    time_source: Arc<dyn SystemTimeSource>,
    catalog: Catalog,
}

#[component(pub)]
#[interface(dyn TaskExecutor)]
#[scope(Singleton)]
impl TaskExecutorInMemory {
    pub fn new(
        task_sched: Arc<dyn TaskScheduler>,
        event_store: Arc<dyn TaskSystemEventStore>,
        event_bus: Arc<EventBus>,
        time_source: Arc<dyn SystemTimeSource>,
        catalog: Catalog,
    ) -> Self {
        Self {
            task_sched,
            event_store,
            event_bus,
            time_source,
            catalog,
        }
    }

    async fn publish_task_running(&self, task_id: TaskID) -> Result<(), InternalError> {
        self.event_bus
            .dispatch_event(TaskEventRunning {
                event_time: self.time_source.now(),
                task_id,
            })
            .await
    }

    async fn publish_task_finished(
        &self,
        task_id: TaskID,
        outcome: TaskOutcome,
    ) -> Result<(), InternalError> {
        self.event_bus
            .dispatch_event(TaskEventFinished {
                event_time: self.time_source.now(),
                task_id,
                outcome,
            })
            .await
    }
}

#[async_trait::async_trait]
impl TaskExecutor for TaskExecutorInMemory {
    // TODO: Error and panic handling strategy
    async fn run(&self) -> Result<(), InternalError> {
        loop {
            let task_id = self.task_sched.take().await.int_err()?;
            let mut task = Task::load(task_id, self.event_store.as_ref())
                .await
                .int_err()?;

            tracing::info!(
                %task_id,
                logical_plan = ?task.logical_plan,
                "Executing task",
            );

            self.publish_task_running(task.task_id).await?;

            let outcome = match &task.logical_plan {
                LogicalPlan::UpdateDataset(upd) => {
                    let pull_svc = self.catalog.get_one::<dyn PullService>().int_err()?;
                    let maybe_pull_result = pull_svc
                        .pull(&upd.dataset_id.as_any_ref(), PullOptions::default(), None)
                        .await;

                    match maybe_pull_result {
                        Ok(pull_result) => {
                            TaskOutcome::Success(TaskResult::PullResult(pull_result))
                        }
                        Err(_) => TaskOutcome::Failed,
                    }
                }
                LogicalPlan::Probe(Probe {
                    busy_time,
                    end_with_outcome,
                    ..
                }) => {
                    if let Some(busy_time) = busy_time {
                        tokio::time::sleep(*busy_time).await;
                    }
                    end_with_outcome
                        .clone()
                        .unwrap_or(TaskOutcome::Success(TaskResult::Empty))
                }
            };

            tracing::info!(
                %task_id,
                logical_plan = ?task.logical_plan,
                ?outcome,
                "Task finished",
            );

            // Refresh the task in case it was updated concurrently (e.g. late cancellation)
            task.update(self.event_store.as_ref()).await.int_err()?;
            task.finish(self.time_source.now(), outcome.clone())
                .int_err()?;
            task.save(self.event_store.as_ref()).await.int_err()?;

            self.publish_task_finished(task.task_id, outcome).await?;
        }
    }
}
