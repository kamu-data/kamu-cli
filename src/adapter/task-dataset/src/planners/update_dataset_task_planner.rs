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
use kamu_core::{
    PollingIngestOptions,
    PullOptions,
    PullRequest,
    PullRequestPlanner,
    TenancyConfig,
};
use kamu_task_system::*;

use super::task_planning::planning_failed;
use crate::{LogicalPlanDatasetUpdate, TaskDefinitionDatasetUpdate};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
#[dill::meta(TaskDefinitionPlannerMeta {
    logic_plan_type: LogicalPlanDatasetUpdate::TYPE_ID,
})]
pub struct UpdateDatasetTaskPlanner {
    catalog: dill::Catalog,
    tenancy_config: Arc<TenancyConfig>,
}

impl UpdateDatasetTaskPlanner {
    #[transactional_method1(pull_request_planner: Arc<dyn PullRequestPlanner>)]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_update(
        &self,
        args: &LogicalPlanDatasetUpdate,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        let pull_options = PullOptions {
            ingest_options: PollingIngestOptions {
                fetch_uncacheable: args.fetch_uncacheable,
                ..Default::default()
            },
            ..Default::default()
        };

        let pull_job = pull_request_planner
            .build_pull_plan(
                PullRequest::local(args.dataset_id.as_local_ref()),
                &pull_options,
                *self.tenancy_config,
            )
            .await
            .map_err(|e| planning_failed(&e, e.is_recoverable()))?;

        Ok(TaskDefinition::new(TaskDefinitionDatasetUpdate {
            ingest_options: pull_options.ingest_options,
            sync_options: pull_options.sync_options,
            job: pull_job
                .try_into()
                .map_err(|e| planning_failed(&e, false))?,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for UpdateDatasetTaskPlanner {
    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, PrepareTaskDefinitionError> {
        assert_eq!(
            logical_plan.plan_type,
            LogicalPlanDatasetUpdate::TYPE_ID,
            "UpdateDatasetTaskPlanner received an unsupported logical plan type: {logical_plan:?}",
        );

        let update_plan = LogicalPlanDatasetUpdate::from_logical_plan(logical_plan)?;
        self.plan_update(&update_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
