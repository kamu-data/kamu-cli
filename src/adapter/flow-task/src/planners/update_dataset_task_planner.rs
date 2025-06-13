// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common_macros::transactional_method2;
use internal_error::InternalError;
use kamu_core::{
    PollingIngestOptions,
    PullOptions,
    PullRequest,
    PullRequestPlanner,
    TenancyConfig,
};
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;

use crate::TaskDefinitionDatasetUpdate;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskDefinitionPlanner)]
pub struct UpdateDatasetTaskPlanner {
    catalog: dill::Catalog,
    tenancy_config: Arc<TenancyConfig>,
}

impl UpdateDatasetTaskPlanner {
    #[transactional_method2(
        pull_request_planner: Arc<dyn PullRequestPlanner>,
        dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>
    )]
    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_update(
        &self,
        args: &LogicalPlanUpdateDataset,
    ) -> Result<TaskDefinition, InternalError> {
        let dataset_env_vars = dataset_env_vars_svc
            .get_all_dataset_env_vars_by_dataset_id(&args.dataset_id, None)
            .await
            .map(|listing| listing.list)
            .int_err()?;

        let pull_options = PullOptions {
            ingest_options: PollingIngestOptions {
                dataset_env_vars: dataset_env_vars
                    .into_iter()
                    .map(|dataset_env_var| (dataset_env_var.key.clone(), dataset_env_var))
                    .collect::<HashMap<String, DatasetEnvVar>>(),
                fetch_uncacheable: args.fetch_uncacheable,
                ..Default::default()
            },
            ..Default::default()
        };

        let plan_res = pull_request_planner
            .build_pull_plan(
                PullRequest::local(args.dataset_id.as_local_ref()),
                &pull_options,
                *self.tenancy_config,
            )
            .await;

        match plan_res {
            Ok(pull_job) => {
                pull_job.detach_from_transaction();

                Ok(TaskDefinition::new(TaskDefinitionDatasetUpdate {
                    pull_options,
                    pull_job,
                }))
            }
            Err(e) => {
                assert!(e.result.is_err());
                tracing::error!(
                    args = ?args,
                    error = ?e,
                    "Update failed",
                );
                Err("Update task planning failed".int_err())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for UpdateDatasetTaskPlanner {
    fn supported_task_type(&self) -> &str {
        TaskDefinitionDatasetUpdate::TASK_TYPE
    }

    async fn prepare_task_definition(
        &self,
        _task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        let kamu_task_system::LogicalPlan::UpdateDataset(update_plan) = logical_plan else {
            panic!(
                "UpdateDatasetTaskPlanner received an unsupported logical plan type: \
                 {logical_plan:?}",
            );
        };

        self.plan_update(update_plan).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
