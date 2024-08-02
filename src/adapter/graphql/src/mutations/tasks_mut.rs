// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_task_system as ts;

use crate::prelude::*;
use crate::queries::Task;
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TasksMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Error handling
#[Object]
impl TasksMut {
    /// Requests cancellation of the specified task
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn cancel_task(&self, ctx: &Context<'_>, task_id: TaskID) -> Result<Task> {
        let task_sched = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
        let task_state = task_sched.cancel_task(task_id.into()).await.int_err()?;
        Ok(Task::new(task_state))
    }

    /// Schedules a task to update the specified dataset by performing polling
    /// ingest or a derivative transformation
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_update_dataset_task(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID,
        fetch_uncacheable: bool,
    ) -> Result<Task> {
        let task_sched = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
        let task_state = task_sched
            .create_task(ts::LogicalPlan::UpdateDataset(ts::UpdateDataset {
                dataset_id: dataset_id.into(),
                fetch_uncacheable,
            }))
            .await
            .int_err()?;
        Ok(Task::new(task_state))
    }

    /// Schedules a task to update the specified dataset by performing polling
    /// ingest or a derivative transformation
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_probe_task(
        &self,
        ctx: &Context<'_>,
        dataset_id: Option<DatasetID>,
        busy_time_ms: Option<u64>,
        end_with_outcome: Option<TaskOutcome>,
    ) -> Result<Task> {
        let task_sched = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
        let task_state = task_sched
            .create_task(ts::LogicalPlan::Probe(ts::Probe {
                dataset_id: dataset_id.map(Into::into),
                busy_time: busy_time_ms.map(std::time::Duration::from_millis),
                end_with_outcome: end_with_outcome.map(|o| match o {
                    TaskOutcome::Success => ts::TaskOutcome::Success(ts::TaskResult::Empty),
                    TaskOutcome::Failed => ts::TaskOutcome::Failed(ts::TaskError::Empty),
                    TaskOutcome::Cancelled => ts::TaskOutcome::Cancelled,
                }),
            }))
            .await
            .int_err()?;
        Ok(Task::new(task_state))
    }
}
