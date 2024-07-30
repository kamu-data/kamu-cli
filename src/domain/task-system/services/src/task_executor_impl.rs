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
use event_bus::EventBus;
use kamu_core::{
    CompactionOptions,
    CompactionService,
    DatasetRepository,
    PollingIngestOptions,
    PullError,
    PullOptions,
    PullService,
    SystemTimeSource,
    TransformError,
};
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskExecutorImpl {
    task_sched: Arc<dyn TaskScheduler>,
    event_store: Arc<dyn TaskSystemEventStore>,
    event_bus: Arc<EventBus>,
    time_source: Arc<dyn SystemTimeSource>,
    catalog: Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskExecutor)]
#[scope(Singleton)]
impl TaskExecutorImpl {
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
                LogicalPlan::HardCompactionDataset(hard_compaction_args) => {
                    self.hard_compaction_logical_plan(hard_compaction_args)
                        .await?
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
