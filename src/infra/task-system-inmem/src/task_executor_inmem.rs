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
use kamu_core::{
    DatasetRepository,
    PullOptions,
    PullResult,
    PullService,
    SystemTimeSource,
    TryStreamExtExt,
};
use kamu_task_system::*;
use opendatafabric::{DataSlice, DatasetID, MetadataEvent};
use tokio_stream::StreamExt;

pub struct TaskExecutorInMemory {
    task_sched: Arc<dyn TaskScheduler>,
    event_store: Arc<dyn TaskSystemEventStore>,
    dataset_repository: Arc<dyn DatasetRepository>,
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
        dataset_repository: Arc<dyn DatasetRepository>,
        event_bus: Arc<EventBus>,
        time_source: Arc<dyn SystemTimeSource>,
        catalog: Catalog,
    ) -> Self {
        Self {
            task_sched,
            event_store,
            dataset_repository,
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

    async fn make_dataset_update_result(
        &self,
        dataset_id: &DatasetID,
        pull_result: PullResult,
    ) -> Result<TaskUpdateDatasetResult, InternalError> {
        Ok(match pull_result {
            PullResult::UpToDate => TaskUpdateDatasetResult {
                pull_result,
                num_blocks: 0,
                num_records: 0,
                updated_watermark: None,
            },
            PullResult::Updated { old_head, new_head } => {
                // Scan updated dataset to detect statistics for task system result

                let dataset = self
                    .dataset_repository
                    .get_dataset(&dataset_id.as_local_ref())
                    .await
                    .int_err()?;

                let mut num_blocks = 0;
                let mut num_records = 0;
                let mut updated_watermark = None;

                {
                    let mut block_stream = dataset.as_metadata_chain().iter_blocks_interval(
                        &new_head,
                        old_head.as_ref(),
                        false,
                    );

                    // The watermark seen nearest to new head
                    let mut latest_watermark = None;

                    // Scan blocks (from new head to old head)
                    while let Some((_, block)) = block_stream.try_next().await.int_err()? {
                        // Each block counts
                        num_blocks += 1;

                        // Count added records in data blocks
                        num_records += match &block.event {
                            MetadataEvent::AddData(add_data) => add_data
                                .new_data
                                .as_ref()
                                .map(DataSlice::num_records)
                                .unwrap_or_default(),
                            MetadataEvent::ExecuteTransform(execute_transform) => execute_transform
                                .new_data
                                .as_ref()
                                .map(DataSlice::num_records)
                                .unwrap_or_default(),
                            _ => 0,
                        };

                        // If we haven't decided on the updated watermark yet, analyze watermarks
                        if updated_watermark.is_none() {
                            // Extract watermark of this block, if present
                            let block_watermark = match &block.event {
                                MetadataEvent::AddData(add_data) => add_data.new_watermark,
                                MetadataEvent::ExecuteTransform(execute_transform) => {
                                    execute_transform.new_watermark
                                }
                                _ => None,
                            };
                            if let Some(block_watermark) = block_watermark {
                                // Did we have a watermark already since the start of scanning?
                                if let Some(latest_watermark_ref) = latest_watermark.as_ref() {
                                    // Yes, so if we see a different watermark now, it means it was
                                    // updated in this pull result
                                    if block_watermark != *latest_watermark_ref {
                                        updated_watermark = Some(*latest_watermark_ref);
                                    }
                                } else {
                                    // No, so remember the latest watermark
                                    latest_watermark = Some(block_watermark);
                                }
                            }
                        }
                    }

                    // We have reach the end of pulled interval.
                    // If we've seen some watermark, but not the previous one within the changed
                    // interval, we need to look for the previous watermark earlier
                    if updated_watermark.is_none()
                        && let Some(latest_watermark_ref) = latest_watermark.as_ref()
                    {
                        // Did we have any head before?
                        if let Some(old_head) = &old_head {
                            // Yes, so try locating the previous watermark - earliest different
                            // watermark
                            let prev_different_watermark = dataset
                                .as_metadata_chain()
                                .iter_blocks_interval(old_head, None, false)
                                .filter_data_stream_blocks()
                                .filter_map_ok(|(_, b)| {
                                    b.event.new_watermark.and_then(|new_watermark| {
                                        if new_watermark != *latest_watermark_ref {
                                            Some(new_watermark)
                                        } else {
                                            None
                                        }
                                    })
                                })
                                .try_first()
                                .await
                                .int_err()?;
                            updated_watermark = prev_different_watermark.or(latest_watermark);
                        } else {
                            // It's a first pull, the latest watermark is an update
                            updated_watermark = latest_watermark;
                        }
                    }
                }

                TaskUpdateDatasetResult {
                    pull_result: PullResult::Updated { old_head, new_head },
                    num_blocks,
                    num_records,
                    updated_watermark,
                }
            }
        })
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
                        Ok(pull_result) => TaskOutcome::Success(TaskResult::UpdateDatasetResult(
                            self.make_dataset_update_result(&upd.dataset_id, pull_result)
                                .await?,
                        )),
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
