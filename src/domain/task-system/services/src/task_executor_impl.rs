// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use dill::*;
use kamu_core::{
    CompactionOptions,
    CompactionService,
    DatasetRepository,
    PullOptions,
    PullService,
};
use kamu_task_system::*;
use messaging_outbox::{post_outbox_message, MessageRelevance, Outbox};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskExecutorImpl {
    catalog: Catalog,
    task_sched: Arc<dyn TaskScheduler>,
    time_source: Arc<dyn SystemTimeSource>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskExecutor)]
#[scope(Singleton)]
impl TaskExecutorImpl {
    pub fn new(
        catalog: Catalog,
        task_sched: Arc<dyn TaskScheduler>,
        time_source: Arc<dyn SystemTimeSource>,
    ) -> Self {
        Self {
            catalog,
            task_sched,
            time_source,
        }
    }

    async fn take_task(&self) -> Result<Task, InternalError> {
        let task_id = self.task_sched.take().await.int_err()?;

        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with2(
                |event_store: Arc<dyn TaskSystemEventStore>, outbox: Arc<dyn Outbox>| async move {
                    let task = Task::load(task_id, event_store.as_ref()).await.int_err()?;

                    tracing::info!(
                        %task_id,
                        logical_plan = ?task.logical_plan,
                        "Executing task",
                    );

                    post_outbox_message(
                        outbox.as_ref(),
                        MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                        TaskRunningMessage {
                            event_time: self.time_source.now(),
                            task_id,
                        },
                        MessageRelevance::Essential,
                    )
                    .await?;

                    Ok(task)
                },
            )
            .await
    }

    async fn execute_task(&self, task: &Task) -> Result<TaskOutcome, InternalError> {
        let task_outcome = match &task.logical_plan {
            LogicalPlan::UpdateDataset(upd) => {
                DatabaseTransactionRunner::new(self.catalog.clone())
                    .transactional_with(|pull_svc: Arc<dyn PullService>| async move {
                        let maybe_pull_result = pull_svc
                            .pull(&upd.dataset_id.as_any_ref(), PullOptions::default(), None)
                            .await;

                        Ok(match maybe_pull_result {
                            Ok(pull_result) => TaskOutcome::Success(
                                TaskResult::UpdateDatasetResult(pull_result.into()),
                            ),
                            Err(_) => TaskOutcome::Failed(TaskError::Empty),
                        })
                    })
                    .await?
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
            LogicalPlan::HardCompactionDataset(HardCompactionDataset {
                dataset_id,
                max_slice_size,
                max_slice_records,
                keep_metadata_only,
            }) => {
                let compaction_svc = self.catalog.get_one::<dyn CompactionService>().int_err()?;
                let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().int_err()?;
                let dataset_handle = dataset_repo
                    .resolve_dataset_ref(&dataset_id.as_local_ref())
                    .await
                    .int_err()?;

                let compaction_result = compaction_svc
                    .compact_dataset(
                        &dataset_handle,
                        CompactionOptions {
                            max_slice_size: *max_slice_size,
                            max_slice_records: *max_slice_records,
                            keep_metadata_only: *keep_metadata_only,
                        },
                        None,
                    )
                    .await;

                match compaction_result {
                    Ok(result) => {
                        TaskOutcome::Success(TaskResult::CompactionDatasetResult(result.into()))
                    }
                    Err(err) => {
                        tracing::info!(
                            task_id = %task.task_id,
                            logical_plan = ?task.logical_plan,
                            err = ?err,
                            "Task failed",
                        );
                        TaskOutcome::Failed(TaskError::Empty)
                    }
                }
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

    async fn process_task_outcome(
        &self,
        mut task: Task,
        task_outcome: TaskOutcome,
    ) -> Result<(), InternalError> {
        DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with2(
                |event_store: Arc<dyn TaskSystemEventStore>, outbox: Arc<dyn Outbox>| async move {
                    // Refresh the task in case it was updated concurrently (e.g. late cancellation)
                    task.update(event_store.as_ref()).await.int_err()?;
                    task.finish(self.time_source.now(), task_outcome.clone())
                        .int_err()?;
                    task.save(event_store.as_ref()).await.int_err()?;

                    post_outbox_message(
                        outbox.as_ref(),
                        MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                        TaskFinishedMessage {
                            event_time: self.time_source.now(),
                            task_id: task.task_id,
                            outcome: task_outcome,
                        },
                        MessageRelevance::Essential,
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
    // TODO: Error and panic handling strategy
    async fn run(&self) -> Result<(), InternalError> {
        loop {
            let task = self.take_task().await?;
            let task_outcome = self.execute_task(&task).await?;
            self.process_task_outcome(task, task_outcome).await?;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
