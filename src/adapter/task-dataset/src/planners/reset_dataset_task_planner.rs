// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use kamu_datasets::DatasetRegistry;
use kamu_task_system::*;

use super::task_planning::resolve_task_dataset;
use crate::{LogicalPlanDatasetReset, TaskDefinitionDatasetReset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prepares reset tasks. The runner's `ResetJob` checks the expected old head;
/// a dataset deleted before the task starts fails it here
#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetReset::TYPE_ID,
})]
pub struct ResetDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl ResetDatasetTaskPlanner {
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn resolve_target(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<odf::DatasetHandle, PrepareTaskDefinitionError> {
        resolve_task_dataset(dataset_registry.as_ref(), dataset_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for ResetDatasetTaskPlanner {
    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanDatasetReset::TYPE_ID,
            "ResetDatasetTaskPlanner received an unsupported logical plan type: {logical_plan:?}",
        );

        let args = LogicalPlanDatasetReset::from_logical_plan(logical_plan)?;
        let dataset_handle = self.resolve_target(&args.dataset_id).await?;

        Ok(TaskDefinition::new(TaskDefinitionDatasetReset {
            dataset_handle,
            new_head_hash: args.new_head_hash,
            old_head_hash: args.old_head_hash,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
