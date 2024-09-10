// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::{DatabaseTransactionRunner, PaginationOpts};
use dill::*;
use kamu_task_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskExecutorImpl {
    catalog: Catalog,
    task_logical_plan_runner: Arc<dyn TaskLogicalPlanRunner>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskExecutor)]
#[scope(Singleton)]
impl TaskExecutorImpl {
    pub fn new(
        catalog: Catalog,
        task_logical_plan_runner: Arc<dyn TaskLogicalPlanRunner>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            catalog,
            task_logical_plan_runner,
            time_source,
        }
    }

    async fn run_task_iteration(&self) -> Result<(), InternalError> {
        let task = self.take_task().await?;
        let task_outcome = self.run_task(&task).await?;
        self.process_task_outcome(task, task_outcome).await?;
        Ok(())
    }

    async fn recover_running_tasks(&self) -> Result<(), InternalError> {
        // Recovering tasks means we are re-queing tasks that started running, but got
        // aborted due to server shutdown or crash
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional(
                "TaskExecutor::recover_running_task",
                |target_catalog: Catalog| async move {
                    let task_event_store = target_catalog.get_one::<dyn TaskEventStore>().unwrap();

                    // Total number of running tasks
                    let total_running_tasks = task_event_store.get_count_running_tasks().await?;

                    // Processe them in pages
                    let mut processed_running_tasks = 0;
                    while processed_running_tasks < total_running_tasks {
                        // Load another page
                        use futures::TryStreamExt;
                        let running_task_ids: Vec<_> = task_event_store
                            .get_running_tasks(PaginationOpts {
                                offset: processed_running_tasks,
                                limit: 100,
                            })
                            .try_collect()
                            .await?;

                        for running_task_id in &running_task_ids {
                            // TODO: batch loading of tasks
                            let mut task = Task::load(*running_task_id, task_event_store.as_ref())
                                .await
                                .int_err()?;

                            // Requeue
                            task.requeue(self.time_source.now()).int_err()?;
                            task.save(task_event_store.as_ref()).await.int_err()?;
                        }

                        processed_running_tasks += running_task_ids.len();
                    }

                    Ok(())
                },
            )
            .await
    }

    async fn take_task(&self) -> Result<Task, InternalError> {
        loop {
            let maybe_task = DatabaseTransactionRunner::new(self.catalog.clone())
                .transactional(
                    "TaskExecutor::take_task",
                    |target_catalog: Catalog| async move {
                        let task_scheduler = target_catalog.get_one::<dyn TaskScheduler>().unwrap();
                        let maybe_task = task_scheduler.try_take().await.int_err()?;
                        let Some(task) = maybe_task else {
                            return Ok(None);
                        };

                        tracing::info!(
                            task_id = %task.task_id,
                            logical_plan = ?task.logical_plan,
                            "Executing task",
                        );

                        let outbox = target_catalog.get_one::<dyn Outbox>().unwrap();
                        outbox
                            .post_message(
                                MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                                TaskProgressMessage::running(
                                    self.time_source.now(),
                                    task.task_id,
                                    task.metadata.clone(),
                                ),
                            )
                            .await?;

                        Ok(Some(task))
                    },
                )
                .await?;

            if let Some(task) = maybe_task {
                return Ok(task);
            }

            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        }
    }

    #[tracing::instrument(level = "info", skip_all, fields(task_id = %task.task_id))]
    async fn run_task(&self, task: &Task) -> Result<TaskOutcome, InternalError> {
        let task_outcome = self
            .task_logical_plan_runner
            .run_plan(&task.logical_plan)
            .await?;

        tracing::info!(
            task_id = %task.task_id,
            logical_plan = ?task.logical_plan,
            ?task_outcome,
            "Task finished",
        );

        Ok(task_outcome)
    }

    async fn process_task_outcome(
        &self,
        mut task: Task,
        task_outcome: TaskOutcome,
    ) -> Result<(), InternalError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with2(
                "TaskExecutor::process_task_outcome",
                |event_store: Arc<dyn TaskEventStore>, outbox: Arc<dyn Outbox>| async move {
                    // Refresh the task in case it was updated concurrently (e.g. late cancellation)
                    task.update(event_store.as_ref()).await.int_err()?;
                    task.finish(self.time_source.now(), task_outcome.clone())
                        .int_err()?;
                    task.save(event_store.as_ref()).await.int_err()?;

                    outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                            TaskProgressMessage::finished(
                                self.time_source.now(),
                                task.task_id,
                                task.metadata.clone(),
                                task_outcome,
                            ),
                        )
                        .await
                },
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskExecutor for TaskExecutorImpl {
    #[tracing::instrument(level = "info", skip_all)]
    async fn pre_run(&self) -> Result<(), InternalError> {
        self.recover_running_tasks().await?;
        Ok(())
    }

    // TODO: Error and panic handling strategy
    async fn run(&self) -> Result<(), InternalError> {
        loop {
            self.run_task_iteration().await?;
        }
    }

    /// Runs single task only, blocks until it is available (for tests only!)
    #[tracing::instrument(level = "info", skip_all)]
    async fn run_single_task(&self) -> Result<(), InternalError> {
        self.run_task_iteration().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
