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
use kamu_core::CompactionOptions;
use kamu_datasets::DatasetRegistry;
use kamu_task_system::*;

use super::task_planning::resolve_task_dataset;
use crate::{
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetResetToMetadata,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Prepares hard compaction and reset-to-metadata tasks, which differ only in
/// their compaction options. The runner's `CompactionJob` plans the
/// compaction; a dataset deleted before the task starts fails it here
#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetHardCompact::TYPE_ID,
})]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetResetToMetadata::TYPE_ID,
})]
pub struct CompactionDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl CompactionDatasetTaskPlanner {
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
impl TaskDefinitionPlanner for CompactionDatasetTaskPlanner {
    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        match logical_plan.plan_type.as_str() {
            LogicalPlanDatasetHardCompact::TYPE_ID => {
                let args = LogicalPlanDatasetHardCompact::from_logical_plan(logical_plan)?;
                let target = self.resolve_target(&args.dataset_id).await?;

                Ok(TaskDefinition::new(TaskDefinitionDatasetHardCompact {
                    target,
                    options: CompactionOptions {
                        max_slice_size: args.max_slice_size,
                        max_slice_records: args.max_slice_records,
                        keep_metadata_only: false,
                    },
                }))
            }
            LogicalPlanDatasetResetToMetadata::TYPE_ID => {
                let args = LogicalPlanDatasetResetToMetadata::from_logical_plan(logical_plan)?;
                let target = self.resolve_target(&args.dataset_id).await?;

                Ok(TaskDefinition::new(TaskDefinitionDatasetResetToMetadata {
                    target,
                }))
            }
            unexpected => panic!(
                "CompactionDatasetTaskPlanner received an unsupported logical plan type: \
                 {unexpected}"
            ),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
