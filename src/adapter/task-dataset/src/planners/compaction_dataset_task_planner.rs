// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method2;
use kamu_core::{CompactionOptions, CompactionPlan, CompactionPlanner};
use kamu_datasets::{DatasetRegistry, ResolvedDataset};
use kamu_task_system::*;

use super::task_planning::{planning_failed, resolve_task_dataset};
use crate::{
    LogicalPlanDatasetHardCompact,
    LogicalPlanDatasetResetToMetadata,
    TaskDefinitionDatasetHardCompact,
    TaskDefinitionDatasetResetToMetadata,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans hard compaction and reset-to-metadata tasks, which differ only in
/// their compaction options and in the definition they produce.
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

#[common_macros::method_names_consts]
impl CompactionDatasetTaskPlanner {
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, compaction_planner: Arc<dyn CompactionPlanner>)]
    #[tracing::instrument(name = CompactionDatasetTaskPlanner_plan_compaction, level = "debug", skip_all, fields(%dataset_id, ?options))]
    async fn plan_compaction(
        &self,
        dataset_id: &odf::DatasetID,
        options: CompactionOptions,
    ) -> Result<(ResolvedDataset, CompactionPlan), PrepareTaskDefinitionError> {
        let target = resolve_task_dataset(dataset_registry.as_ref(), dataset_id).await?;

        let compaction_plan = compaction_planner
            .plan_compaction(target.clone(), options, None)
            .await
            .map_err(|e| planning_failed(&e, e.is_recoverable()))?;

        target.detach_from_transaction();

        Ok((target, compaction_plan))
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
                let (target, compaction_plan) = self
                    .plan_compaction(
                        &args.dataset_id,
                        CompactionOptions {
                            max_slice_size: args.max_slice_size,
                            max_slice_records: args.max_slice_records,
                            keep_metadata_only: false,
                        },
                    )
                    .await?;

                Ok(TaskDefinition::new(TaskDefinitionDatasetHardCompact {
                    target,
                    compaction_plan,
                }))
            }
            LogicalPlanDatasetResetToMetadata::TYPE_ID => {
                let args = LogicalPlanDatasetResetToMetadata::from_logical_plan(logical_plan)?;
                let (target, compaction_metadata_only_plan) = self
                    .plan_compaction(
                        &args.dataset_id,
                        CompactionOptions {
                            max_slice_size: None,
                            max_slice_records: None,
                            keep_metadata_only: true,
                        },
                    )
                    .await?;

                Ok(TaskDefinition::new(TaskDefinitionDatasetResetToMetadata {
                    target,
                    compaction_metadata_only_plan,
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
