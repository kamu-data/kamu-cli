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
use kamu_core::{DatasetRegistry, DatasetRegistryExt, ResetPlanner};
use kamu_task_system::*;

use crate::{LogicalPlanDatasetReset, TaskDefinitionDatasetReset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
pub struct ResetDatasetTaskPlanner {
    catalog: dill::Catalog,
}

impl ResetDatasetTaskPlanner {
    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, reset_planner: Arc<dyn ResetPlanner>)]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_reset(
        &self,
        args: &LogicalPlanDatasetReset,
    ) -> Result<TaskDefinition, InternalError> {
        let target = dataset_registry
            .get_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let reset_plan = reset_planner
            .plan_reset(
                target.clone(),
                args.new_head_hash.as_ref(),
                args.old_head_hash.as_ref(),
            )
            .await
            .int_err()?;

        target.detach_from_transaction();

        Ok(TaskDefinition::new(TaskDefinitionDatasetReset {
            dataset_handle: target.take_handle(),
            reset_plan,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for ResetDatasetTaskPlanner {
    fn supported_logic_plan_type(&self) -> &str {
        LogicalPlanDatasetReset::TYPE_ID
    }

    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanDatasetReset::TYPE_ID,
            "ResetDatasetTaskPlanner received an unsupported logical plan type: {logical_plan:?}",
        );

        let reset_plan = LogicalPlanDatasetReset::from_logical_plan(logical_plan)?;
        self.plan_reset(&reset_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
