// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_graphql::*;
use kamu_task_system as ts;

use crate::queries::Task;
use crate::scalars::*;
use crate::utils::from_catalog;

///////////////////////////////////////////////////////////////////////////////

pub struct TasksMutations;

///////////////////////////////////////////////////////////////////////////////

#[Object]
impl TasksMutations {
    async fn crate_update_dataset_task(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID,
    ) -> Result<Task> {
        let task_svc = from_catalog::<dyn ts::TaskService>(ctx).unwrap();
        let task_state = task_svc
            .create_task(ts::LogicalPlan::UpdateDataset(ts::UpdateDataset {
                dataset_id: dataset_id.into(),
            }))
            .await?;
        Ok(Task::new(task_state))
    }
}
