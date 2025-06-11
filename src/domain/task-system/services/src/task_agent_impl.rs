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
use database_common_macros::{transactional_method1, transactional_method2};
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use kamu_task_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;
use tracing::Instrument as _;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskAgentImpl {
    catalog: Catalog,
    task_runner: Arc<dyn TaskRunner>,
    time_source: Arc<dyn SystemTimeSource>,
    agent_config: Arc<TaskAgentConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskAgent)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_TASKS_AGENT_RECOVERY,
    depends_on: &[],
    requires_transaction: false,
})]
#[scope(Singleton)]
impl TaskAgentImpl {
    pub fn new(
        catalog: Catalog,
        task_runner: Arc<dyn TaskRunner>,
        time_source: Arc<dyn SystemTimeSource>,
        agent_config: Arc<TaskAgentConfig>,
    ) -> Self {
        Self {
            catalog,
            task_runner,
            time_source,
            agent_config,
        }
    }

    async fn run_task_iteration(&self) -> Result<(), InternalError> {
        let task = self.take_task().await?;

        let task_outcome = self
            .run_task(&task)
            .instrument(observability::tracing::root_span!(
                "TaskAgent::run_task",
                task_id = %task.task_id,
            ))
            .await?;

        self.process_task_outcome(task, task_outcome).await?;

        Ok(())
    }

    #[transactional_method1(task_event_store: Arc<dyn TaskEventStore>)]
    #[tracing::instrument(level = "info", skip_all)]
    async fn recover_running_tasks(&self) -> Result<(), InternalError> {
        // Recovering tasks means we are re-queuing tasks that started running, but got
        // aborted due to server shutdown or crash

        // Total number of running tasks
        let total_running_tasks = task_event_store.get_count_running_tasks().await?;

        // Process them in pages
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
            let batch_size = running_task_ids.len();

            let tasks = Task::load_multi_simple(running_task_ids, task_event_store.as_ref())
                .await
                .int_err()?;

            for mut task in tasks {
                // Requeue
                task.requeue(self.time_source.now()).int_err()?;
                task.save(task_event_store.as_ref()).await.int_err()?;
            }

            processed_running_tasks += batch_size;
        }

        Ok(())
    }

    async fn take_task(&self) -> Result<Task, InternalError> {
        loop {
            let maybe_task = self.take_task_non_blocking().await?;

            if let Some(task) = maybe_task {
                return Ok(task);
            }

            self.time_source
                .sleep(self.agent_config.task_checking_interval)
                .await;
        }
    }

    #[transactional_method2(task_scheduler: Arc<dyn TaskScheduler>, outbox: Arc<dyn Outbox>)]
    async fn take_task_non_blocking(&self) -> Result<Option<Task>, InternalError> {
        let maybe_task = task_scheduler.try_take().await.int_err()?;
        let Some(task) = maybe_task else {
            return Ok(None);
        };

        tracing::debug!(task_id = %task.task_id, "Received next task from scheduler");

        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                TaskProgressMessage::running(
                    self.time_source.now(),
                    task.task_id,
                    task.metadata.clone(),
                ),
            )
            .await?;

        Ok(Some(task))
    }

    async fn run_task(&self, task: &Task) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(
            task_id = %task.task_id,
            logical_plan = ?task.logical_plan,
            "Preparing task to run",
        );

        // Prepare task definition (requires transaction)
        let task_definition = match DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |task_definition_planner: Arc<dyn TaskDefinitionPlanner>| async move {
                    task_definition_planner
                        .prepare_task_definition(task.task_id, &task.logical_plan)
                        .await
                },
            )
            .await
        {
            Ok(task_definition) => task_definition,
            Err(e) => {
                tracing::error!(
                    task = ?task,
                    error = ?e,
                    error_msg = %e,
                    "Task definition preparation failed"
                );
                return Ok(TaskOutcome::Failed(TaskError::Empty));
            }
        };

        // Run task via definition
        let task_run_result = self.task_runner.run_task(task_definition).await;

        // Deal with errors: we should not interrupt the main loop if task fails
        let task_outcome = match task_run_result {
            Ok(outcome) => outcome,
            Err(e) => {
                // No useful task result, but at least the error logged
                tracing::error!(
                    task = ?task,
                    error = ?e,
                    error_msg = %e,
                    "Task run failed"
                );
                TaskOutcome::Failed(TaskError::Empty)
            }
        };

        tracing::info!(
            task_id = %task.task_id,
            logical_plan = ?task.logical_plan,
            ?task_outcome,
            "Task finished",
        );

        Ok(task_outcome)
    }

    #[transactional_method2(event_store: Arc<dyn TaskEventStore>, outbox: Arc<dyn Outbox>)]
    async fn process_task_outcome(
        &self,
        mut task: Task,
        task_outcome: TaskOutcome,
    ) -> Result<(), InternalError> {
        // Refresh the task in case it was updated concurrently (e.g. late cancellation)
        task.update(event_store.as_ref()).await.int_err()?;
        task.finish(self.time_source.now(), task_outcome.clone())
            .int_err()?;
        task.save(event_store.as_ref()).await.int_err()?;

        outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                TaskProgressMessage::finished(
                    self.time_source.now(),
                    task.task_id,
                    task.metadata.clone(),
                    task_outcome,
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskAgent for TaskAgentImpl {
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

#[async_trait::async_trait]
impl InitOnStartup for TaskAgentImpl {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        self.recover_running_tasks().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
