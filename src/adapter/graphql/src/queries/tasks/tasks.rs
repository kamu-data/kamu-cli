// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::TryStreamExt;
use kamu_task_system as ts;

use super::Task;
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct Tasks;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[Object]
impl Tasks {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns current state of a given task
    async fn get_task(&self, ctx: &Context<'_>, task_id: TaskID) -> Result<Option<Task>> {
        let task_sched = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();
        match task_sched.get_task(task_id.into()).await {
            Ok(task_state) => Ok(Some(Task::new(task_state))),
            Err(ts::GetTaskError::NotFound(_)) => Ok(None),
            Err(err) => Err(err.int_err().into()),
        }
    }

    /// Returns states of tasks associated with a given dataset ordered by
    /// creation time from newest to oldest
    // TODO: reconsider performance impact
    async fn list_tasks_by_dataset(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<TaskConnection> {
        let task_sched = from_catalog::<dyn ts::TaskScheduler>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let tasks_listing = task_sched
            .list_tasks_by_dataset(
                &dataset_id,
                ts::TaskPaginationOpts {
                    offset: page * per_page,
                    limit: per_page,
                },
            )
            .await
            .int_err()?;

        let nodes: Vec<_> = tasks_listing.stream.map_ok(Task::new).try_collect().await?;
        let total_count = tasks_listing.total_count;

        // TODO: We set total to len + 1 to indicate there is a next page.
        // We should replace this with unbounded size connection.
        Ok(TaskConnection::new(nodes, page, per_page, total_count))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(Task, TaskConnection, TaskEdge);
