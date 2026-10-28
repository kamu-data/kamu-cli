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
use kamu_core::{CompactionOptions, CompactionPlanner};
use kamu_datasets::DatasetRegistry;
use kamu_task_system::*;

use super::task_planning::{planning_failed, resolve_task_dataset};
use crate::{LogicalPlanDatasetHardCompact, TaskDefinitionDatasetHardCompact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetHardCompact::TYPE_ID,
})]
pub struct HardCompactDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl HardCompactDatasetTaskPlanner {
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, compaction_planner: Arc<dyn CompactionPlanner>)]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_hard_compaction(
        &self,
        args: &LogicalPlanDatasetHardCompact,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        let target = resolve_task_dataset(dataset_registry.as_ref(), &args.dataset_id).await?;

        let compaction_options = CompactionOptions {
            max_slice_size: args.max_slice_size,
            max_slice_records: args.max_slice_records,
            keep_metadata_only: false,
        };

        let compaction_plan = compaction_planner
            .plan_compaction(target.clone(), compaction_options, None)
            .await
            .map_err(|e| planning_failed(&e, e.is_recoverable()))?;

        target.detach_from_transaction();

        Ok(TaskDefinition::new(TaskDefinitionDatasetHardCompact {
            target,
            compaction_plan,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for HardCompactDatasetTaskPlanner {
    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanDatasetHardCompact::TYPE_ID,
            "HardCompactDatasetTaskPlanner received an unsupported logical plan type: \
             {logical_plan:?}",
        );

        let compact_plan = LogicalPlanDatasetHardCompact::from_logical_plan(logical_plan)?;
        self.plan_hard_compaction(&compact_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
