// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::DatabaseTransactionRunner;
use dill::*;
use kamu_core::{
    CompactionOptions,
    CompactionService,
    DatasetRepository,
    PollingIngestOptions,
    PullError,
    PullOptions,
    PullService,
    ResetError,
    ResetService,
    TransformError,
};
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;
use messaging_outbox::{Outbox, OutboxExt};
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

                    outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                            TaskProgressMessage::running(self.time_source.now(), task_id),
                        )
                        .await?;

                    Ok(task)
                },
            )
            .await
    }

    async fn execute_task(&self, task: &Task) -> Result<TaskOutcome, InternalError> {
        let task_outcome = match &task.logical_plan {
            LogicalPlan::UpdateDataset(upd) => self.update_dataset_logical_plan(upd).await?,
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
            LogicalPlan::Reset(reset_args) => self.reset_dataset_logical_plan(reset_args).await?,
            LogicalPlan::HardCompactionDataset(hard_compaction_args) => {
                self.hard_compaction_logical_plan(hard_compaction_args)
                    .await?
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

                    outbox
                        .post_message(
                            MESSAGE_PRODUCER_KAMU_TASK_EXECUTOR,
                            TaskProgressMessage::finished(
                                self.time_source.now(),
                                task.task_id,
                                task_outcome,
                            ),
                        )
                        .await
                },
            )
            .await?;

        Ok(())
    }

    async fn update_dataset_logical_plan(
        &self,
        update_dataset_args: &UpdateDataset,
    ) -> Result<TaskOutcome, InternalError> {
        let dataset_env_vars = DatabaseTransactionRunner::new(self.catalog.clone())
            .transactional_with(
                |dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>| async move {
                    let dataset_env_vars = dataset_env_vars_svc
                        .get_all_dataset_env_vars_by_dataset_id(
                            &update_dataset_args.dataset_id,
                            None,
                        )
                        .await
                        .int_err()?;
                    Ok(dataset_env_vars.list)
                },
            )
            .await?;
        let dataset_env_vars_hash_map = dataset_env_vars
            .into_iter()
            .map(|dataset_env_var| (dataset_env_var.key.clone(), dataset_env_var))
            .collect::<HashMap<String, DatasetEnvVar>>();
        let pull_options = PullOptions {
            ingest_options: PollingIngestOptions {
                dataset_env_vars: dataset_env_vars_hash_map,
                ..Default::default()
            },
            ..Default::default()
        };

        let pull_svc = self.catalog.get_one::<dyn PullService>().int_err()?;
        let maybe_pull_result = pull_svc
            .pull(
                &update_dataset_args.dataset_id.as_any_ref(),
                pull_options,
                None,
            )
            .await;

        match maybe_pull_result {
            Ok(pull_result) => Ok(TaskOutcome::Success(TaskResult::UpdateDatasetResult(
                pull_result.into(),
            ))),
            Err(err) => match err {
                PullError::TransformError(TransformError::InvalidInterval(_)) => {
                    Ok(TaskOutcome::Failed(TaskError::UpdateDatasetError(
                        UpdateDatasetTaskError::RootDatasetCompacted(RootDatasetCompactedError {
                            dataset_id: update_dataset_args.dataset_id.clone(),
                        }),
                    )))
                }
                _ => Ok(TaskOutcome::Failed(TaskError::Empty)),
            },
        }
    }

    async fn reset_dataset_logical_plan(
        &self,
        reset_dataset_args: &ResetDataset,
    ) -> Result<TaskOutcome, InternalError> {
        let reset_svc = self.catalog.get_one::<dyn ResetService>().int_err()?;
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().int_err()?;
        let dataset_handle = dataset_repo
            .resolve_dataset_ref(&reset_dataset_args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let reset_result_maybe = reset_svc
            .reset_dataset(
                &dataset_handle,
                reset_dataset_args.new_head_hash.as_ref(),
                reset_dataset_args.old_head_hash.as_ref(),
            )
            .await;
        match reset_result_maybe {
            Ok(new_head) => Ok(TaskOutcome::Success(TaskResult::ResetDatasetResult(
                TaskResetDatasetResult { new_head },
            ))),
            Err(err) => match err {
                ResetError::BlockNotFound(_) => Ok(TaskOutcome::Failed(
                    TaskError::ResetDatasetError(ResetDatasetTaskError::ResetHeadNotFound),
                )),
                _ => Ok(TaskOutcome::Failed(TaskError::Empty)),
            },
        }
    }

    async fn hard_compaction_logical_plan(
        &self,
        hard_compaction_args: &HardCompactionDataset,
    ) -> Result<TaskOutcome, InternalError> {
        let compaction_svc = self.catalog.get_one::<dyn CompactionService>().int_err()?;
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().int_err()?;
        let dataset_handle = dataset_repo
            .resolve_dataset_ref(&hard_compaction_args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_result = compaction_svc
            .compact_dataset(
                &dataset_handle,
                CompactionOptions {
                    max_slice_size: hard_compaction_args.max_slice_size,
                    max_slice_records: hard_compaction_args.max_slice_records,
                    keep_metadata_only: hard_compaction_args.keep_metadata_only,
                },
                None,
            )
            .await;

        match compaction_result {
            Ok(result) => Ok(TaskOutcome::Success(TaskResult::CompactionDatasetResult(
                result.into(),
            ))),
            Err(_) => Ok(TaskOutcome::Failed(TaskError::Empty)),
        }
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
