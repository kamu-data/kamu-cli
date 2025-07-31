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
use kamu_core::{CompactionOptions, CompactionPlanner, DatasetRegistry, DatasetRegistryExt};
use kamu_task_system::*;

use crate::{LogicalPlanDatasetResetToMetadata, TaskDefinitionDatasetHardCompact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetResetToMetadata::TYPE_ID,
})]
pub struct ResetToMetadataDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl ResetToMetadataDatasetTaskPlanner {
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, compaction_planner: Arc<dyn CompactionPlanner>)]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_reset_to_metadata(
        &self,
        args: &LogicalPlanDatasetResetToMetadata,
    ) -> Result<TaskDefinition, InternalError> {
        let target = dataset_registry
            .get_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_options = CompactionOptions {
            max_slice_size: None,
            max_slice_records: None,
            keep_metadata_only: true,
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
impl TaskDefinitionPlanner for ResetToMetadataDatasetTaskPlanner {
    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanDatasetResetToMetadata::TYPE_ID,
            "ResetToMetadataDatasetTaskPlanner received an unsupported logical plan type: \
             {logical_plan:?}",
        );

        let reset_plan = LogicalPlanDatasetResetToMetadata::from_logical_plan(logical_plan)?;
        self.plan_reset_to_metadata(&reset_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
