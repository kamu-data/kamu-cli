// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_utils::BackgroundAgent;
use database_common::PaginationOpts;
use database_common_macros::transactional_method2;
use dill::*;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use kamu_task_system::*;
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;
use tracing::Instrument as _;
use wakeup_listener::{WakeupListener, WakeupListenerConfig};

use crate::TaskAgentMetrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[scope(Singleton)]
#[interface(dyn BackgroundAgent)]
#[interface(dyn TaskAgent)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_TASKS_AGENT_RECOVERY,
    depends_on: &[],
    requires_transaction: false,
})]
pub struct TaskAgentImpl {
    catalog: CatalogWeakRef,
    time_source: Arc<dyn SystemTimeSource>,
    wakeup_config: Arc<WakeupListenerConfig>,
    task_queue_wakeup_source: Arc<dyn TaskQueueWakeupSource>,
    metrics: Arc<TaskAgentMetrics>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl TaskAgentImpl {
    fn get_task_planner_for(
        &self,
        plan: &LogicalPlan,
    ) -> Result<Arc<dyn TaskDefinitionPlanner>, InternalError> {
        let catalog = self.catalog.upgrade();

        catalog
            .builders_for_with_meta::<dyn TaskDefinitionPlanner, _>(
                |meta: &TaskDefinitionPlannerMeta| meta.logic_plan_type == plan.plan_type.as_str(),
            )
            .next()
            .map(|builder| builder.get(&catalog))
            .transpose()
            .int_err()?
            .ok_or_else(|| {
                InternalError::new(format!(
                    "Task definition planner for type '{}' not found",
                    plan.plan_type
                ))
            })
    }

    fn get_task_runner_for(
        &self,
        task_definition: &TaskDefinition,
    ) -> Result<Arc<dyn TaskRunner>, InternalError> {
        let catalog = self.catalog.upgrade();

        catalog
            .builders_for_with_meta::<dyn TaskRunner, _>(|meta: &TaskRunnerMeta| {
                meta.task_type == task_definition.task_type()
            })
            .next()
            .map(|builder| builder.get(&catalog))
            .transpose()
            .int_err()?
            .ok_or_else(|| {
                InternalError::new(format!(
                    "Task runner for type '{}' not found",
                    task_definition.task_type()
                ))
            })
    }

    async fn run_task_iteration(
        &self,
        wakeup_listener: &dyn WakeupListener,
    ) -> Result<(), InternalError> {
        let task = self.take_task(wakeup_listener).await?;
        self.metrics.on_task_started(
            task.timing.created_at,
            task.timing.ran_at.unwrap_or_else(|| self.time_source.now()),
        );

        let task_outcome = self
            .run_task(&task)
            .instrument(observability::tracing::root_span!(
                "TaskAgent::run_task",
                task_id = %task.task_id,
            ))
            .await?;

        let task = self.process_task_outcome(task, task_outcome).await?;
        if let Some(outcome) = &task.outcome {
            let now = self.time_source.now();
            self.metrics.on_task_finished(
                &task.logical_plan.plan_type,
                outcome,
                task.timing.ran_at.unwrap_or(now),
                task.timing.finished_at.unwrap_or(now),
            );
        }

        Ok(())
    }

    #[transactional_method2(task_event_store: Arc<dyn TaskEventStore>, outbox: Arc<dyn Outbox>)]
    #[tracing::instrument(level = "info", skip_all)]
    async fn recover_running_tasks(&self) -> Result<(), InternalError> {
        // Tasks interrupted by a shutdown or crash are requeued, or finished if
        // cancelled meanwhile. Each leaves the running set, so re-read page one
        loop {
            use futures::TryStreamExt;
            let running_task_ids: Vec<_> = task_event_store
                .get_running_tasks(PaginationOpts {
                    offset: 0,
                    limit: 100,
                })
                .try_collect()
                .await?;
            if running_task_ids.is_empty() {
                break;
            }

            let mut tasks = Task::load_multi_simple(&running_task_ids, task_event_store.as_ref())
                .await
                .int_err()?;

            let now = self.time_source.now();
            for task in &mut tasks {
                if task.timing.cancellation_requested_at.is_some() {
                    task.finish(now, TaskOutcome::Cancelled).int_err()?;
                } else {
                    task.requeue(now).int_err()?;
                }
            }

            Task::save_multi(&mut tasks, task_event_store.as_ref())
                .await
                .int_err()?;

            for task in &tasks {
                if task.timing.cancellation_requested_at.is_some() {
                    outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_TASK_AGENT,
                            TaskProgressMessage::finished(
                                now,
                                task.task_id,
                                task.metadata.clone(),
                                TaskOutcome::Cancelled,
                            ),
                        )
                        .await?;

                    tracing::info!(task_id = %task.task_id, "Interrupted cancelled task finished");
                }
            }
        }

        Ok(())
    }

    async fn take_task(&self, wakeup_listener: &dyn WakeupListener) -> Result<Task, InternalError> {
        loop {
            let maybe_task = match self.take_task_non_blocking().await {
                Ok(maybe_task) => maybe_task,
                Err(TakeTaskError::ConcurrentModification { task_id }) => {
                    // Rolled back, and the queue already reflects the change
                    tracing::info!(%task_id, "Task changed while being taken, retrying");
                    continue;
                }
                Err(TakeTaskError::Internal(e)) => return Err(e),
            };

            if let Some(task) = maybe_task {
                // Back-to-back tasks leave no room for waits
                wakeup_listener.heartbeat();
                return Ok(task);
            }

            // Signals are only hints, so the queue is re-checked on any wakeup
            let hint = wakeup_listener
                .wait_wake(
                    self.wakeup_config.max_listening_timeout,
                    self.wakeup_config.min_debounce_interval,
                )
                .await?;
            tracing::debug!(hint = ?hint, "Agent woke up with a hint");
        }
    }

    #[transactional_method2(task_scheduler: Arc<dyn TaskScheduler>, outbox: Arc<dyn Outbox>)]
    async fn take_task_non_blocking(&self) -> Result<Option<Task>, TakeTaskError> {
        let maybe_task = task_scheduler.try_take().await?;
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

        // Find a planner and build a task definition
        let task_planner = self.get_task_planner_for(&task.logical_plan)?;
        let task_definition = match task_planner
            .prepare_task_definition(task.task_id, &task.logical_plan)
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
                return Ok(TaskOutcome::Failed(TaskError::empty_recoverable()));
            }
        };

        // Find a runner and run task via definition
        let task_runner = self.get_task_runner_for(&task_definition)?;
        let task_run_result = task_runner.run_task(task_definition).await;

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
                TaskOutcome::Failed(TaskError::empty_recoverable())
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
    ) -> Result<Task, InternalError> {
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

        Ok(task)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for TaskAgentImpl {
    fn agent_name(&self) -> &'static str {
        TASK_AGENT_NAME
    }

    /// Runs the update main loop
    async fn run(&self) -> Result<(), InternalError> {
        // Kept across iterations, so that no change is missed between them
        let wakeup_listener = self.task_queue_wakeup_source.new_wakeup_listener();

        // TODO: Error and panic handling strategy
        loop {
            self.run_task_iteration(wakeup_listener.as_ref()).await?;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskAgent for TaskAgentImpl {
    /// Runs single task only, blocks until it is available (for tests only!)
    #[tracing::instrument(level = "info", skip_all)]
    async fn run_single_task(&self) -> Result<(), InternalError> {
        let wakeup_listener = self.task_queue_wakeup_source.new_wakeup_listener();
        self.run_task_iteration(wakeup_listener.as_ref()).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for TaskAgentImpl {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        use dill::BuilderExt;

        let catalog = self.catalog.upgrade();
        let plan_types: Vec<&'static str> = catalog
            .builders_for::<dyn TaskDefinitionPlanner>()
            .flat_map(|builder| {
                builder
                    .metadata_get_all::<TaskDefinitionPlannerMeta>()
                    .into_iter()
                    .map(|meta| meta.logic_plan_type)
                    .collect::<Vec<_>>()
            })
            .collect();
        self.metrics.init(plan_types.into_iter());

        self.recover_running_tasks().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
