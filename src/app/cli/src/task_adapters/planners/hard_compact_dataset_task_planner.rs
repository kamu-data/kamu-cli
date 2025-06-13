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
use internal_error::InternalError;
use kamu::domain::{CompactionOptions, CompactionPlanner, DatasetRegistry, DatasetRegistryExt};
use kamu_task_system::*;

use crate::task_adapters::TaskDefinitionDatasetHardCompact;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
pub struct HardCompactDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl HardCompactDatasetTaskPlanner {
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, compaction_planner: Arc<dyn CompactionPlanner>)]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_hard_compaction(
        &self,
        args: &LogicalPlanHardCompactDataset,
    ) -> Result<TaskDefinition, InternalError> {
        let target = dataset_registry
            .get_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_options = CompactionOptions {
            max_slice_size: args.max_slice_size,
            max_slice_records: args.max_slice_records,
            keep_metadata_only: args.keep_metadata_only,
        };

        let compaction_plan = compaction_planner
            .plan_compaction(target.clone(), compaction_options, None)
            .await
            .int_err()?;

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
    fn supported_task_type(&self) -> &str {
        TaskDefinitionDatasetHardCompact::TASK_TYPE
    }

    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        let kamu_task_system::LogicalPlan::HardCompactDataset(compact_plan) = logical_plan else {
            panic!(
                "HardCompactDatasetTaskPlanner received an unsupported logical plan type: \
                 {logical_plan:?}",
            );
        };

        self.plan_hard_compaction(compact_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
