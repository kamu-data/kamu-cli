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

use dill::*;
use internal_error::InternalError;
use kamu_core::*;
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskDefinitionPlannerImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>,
    pull_request_planner: Arc<dyn PullRequestPlanner>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    reset_planner: Arc<dyn ResetPlanner>,
    tenancy_config: Arc<TenancyConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskDefinitionPlanner)]
impl TaskDefinitionPlannerImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>,
        pull_request_planner: Arc<dyn PullRequestPlanner>,
        compaction_planner: Arc<dyn CompactionPlanner>,
        reset_planner: Arc<dyn ResetPlanner>,
        tenancy_config: Arc<TenancyConfig>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_env_vars_svc,
            pull_request_planner,
            compaction_planner,
            reset_planner,
            tenancy_config,
        }
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?probe_plan))]
    async fn plan_probe(
        &self,
        probe_plan: &LogicalPlanProbe,
    ) -> Result<TaskDefinition, InternalError> {
        Ok(TaskDefinition::Probe(TaskDefinitionProbe {
            probe: probe_plan.clone(),
        }))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_update(
        &self,
        args: &LogicalPlanUpdateDataset,
    ) -> Result<TaskDefinition, InternalError> {
        let dataset_env_vars = self
            .dataset_env_vars_svc
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

        let plan_res = self
            .pull_request_planner
            .build_pull_plan(
                PullRequest::local(args.dataset_id.as_local_ref()),
                &pull_options,
                *self.tenancy_config,
            )
            .await;

        match plan_res {
            Ok(pull_job) => {
                pull_job.detach_from_transaction();

                Ok(TaskDefinition::Update(TaskDefinitionUpdate {
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

    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_reset(
        &self,
        args: &LogicalPlanResetDataset,
    ) -> Result<TaskDefinition, InternalError> {
        let target = self
            .dataset_registry
            .get_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let reset_plan = self
            .reset_planner
            .plan_reset(
                target.clone(),
                args.new_head_hash.as_ref(),
                args.old_head_hash.as_ref(),
            )
            .await
            .int_err()?;

        target.detach_from_transaction();

        Ok(TaskDefinition::Reset(TaskDefinitionReset {
            dataset_handle: target.take_handle(),
            reset_plan,
        }))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?args))]
    async fn plan_hard_compaction(
        &self,
        args: &LogicalPlanHardCompactDataset,
    ) -> Result<TaskDefinition, InternalError> {
        let target = self
            .dataset_registry
            .get_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_options = CompactionOptions {
            max_slice_size: args.max_slice_size,
            max_slice_records: args.max_slice_records,
            keep_metadata_only: args.keep_metadata_only,
        };

        let compaction_plan = self
            .compaction_planner
            .plan_compaction(target.clone(), compaction_options, None)
            .await
            .int_err()?;

        target.detach_from_transaction();

        Ok(TaskDefinition::HardCompact(TaskDefinitionHardCompact {
            target,
            compaction_plan,
        }))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskDefinitionPlanner for TaskDefinitionPlannerImpl {
    async fn prepare_task_definition(
        &self,
        logical_plan: &LogicalPlan,
    ) -> Result<TaskDefinition, InternalError> {
        tracing::debug!(?logical_plan, "Preparing task definition");

        let task_definition = match logical_plan {
            LogicalPlan::Probe(probe) => self.plan_probe(probe).await?,
            LogicalPlan::UpdateDataset(upd) => self.plan_update(upd).await?,
            LogicalPlan::ResetDataset(reset) => self.plan_reset(reset).await?,
            LogicalPlan::HardCompactDataset(compaction) => {
                self.plan_hard_compaction(compaction).await?
            }
        };

        Ok(task_definition)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
