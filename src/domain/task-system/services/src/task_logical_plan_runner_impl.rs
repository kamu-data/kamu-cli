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

use database_common_macros::transactional_method1;
use dill::*;
use internal_error::InternalError;
use kamu_core::{
    CompactionOptions,
    CompactionService,
    DatasetRepository,
    PollingIngestOptions,
    PullError,
    PullOptions,
    PullService,
    ResetError,
    ResetService,
    TransformError,
};
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskLogicalPlanRunnerImpl {
    catalog: Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskLogicalPlanRunner)]
impl TaskLogicalPlanRunnerImpl {
    pub fn new(catalog: Catalog) -> Self {
        Self { catalog }
    }

    async fn run_probe(&self, probe_plan: &Probe) -> Result<TaskOutcome, InternalError> {
        if let Some(busy_time) = &probe_plan.busy_time {
            tokio::time::sleep(*busy_time).await;
        }
        Ok(probe_plan
            .end_with_outcome
            .clone()
            .unwrap_or(TaskOutcome::Success(TaskResult::Empty)))
    }

    async fn run_update(&self, args: &UpdateDataset) -> Result<TaskOutcome, InternalError> {
        let dataset_env_vars = self.query_dataset_env_vars(args).await?;
        let dataset_env_vars_hash_map = dataset_env_vars
            .into_iter()
            .map(|dataset_env_var| (dataset_env_var.key.clone(), dataset_env_var))
            .collect::<HashMap<String, DatasetEnvVar>>();

        let pull_options = PullOptions {
            ingest_options: PollingIngestOptions {
                dataset_env_vars: dataset_env_vars_hash_map,
                fetch_uncacheable: args.fetch_uncacheable,
                ..Default::default()
            },
            ..Default::default()
        };

        let pull_svc = self.catalog.get_one::<dyn PullService>().int_err()?;
        let maybe_pull_result = pull_svc
            .pull(&args.dataset_id.as_any_ref(), pull_options, None)
            .await;

        match maybe_pull_result {
            Ok(pull_result) => Ok(TaskOutcome::Success(TaskResult::UpdateDatasetResult(
                TaskUpdateDatasetResult { pull_result },
            ))),
            Err(err) => match err {
                PullError::TransformError(TransformError::InvalidInputInterval(e)) => {
                    Ok(TaskOutcome::Failed(TaskError::UpdateDatasetError(
                        UpdateDatasetTaskError::InputDatasetCompacted(InputDatasetCompactedError {
                            dataset_id: e.input_dataset_id,
                        }),
                    )))
                }
                _ => Ok(TaskOutcome::Failed(TaskError::Empty)),
            },
        }
    }

    #[transactional_method1(dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>)]
    async fn query_dataset_env_vars(
        &self,
        args: &UpdateDataset,
    ) -> Result<Vec<DatasetEnvVar>, InternalError> {
        dataset_env_vars_svc
            .get_all_dataset_env_vars_by_dataset_id(&args.dataset_id, None)
            .await
            .map(|listing| listing.list)
            .int_err()
    }

    async fn run_reset(&self, args: &ResetDataset) -> Result<TaskOutcome, InternalError> {
        let reset_svc = self.catalog.get_one::<dyn ResetService>().int_err()?;
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().int_err()?;
        let dataset_handle = dataset_repo
            .resolve_dataset_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let reset_result_maybe = reset_svc
            .reset_dataset(
                &dataset_handle,
                args.new_head_hash.as_ref(),
                args.old_head_hash.as_ref(),
            )
            .await;
        match reset_result_maybe {
            Ok(new_head) => Ok(TaskOutcome::Success(TaskResult::ResetDatasetResult(
                TaskResetDatasetResult { new_head },
            ))),
            Err(err) => match err {
                ResetError::BlockNotFound(_) => Ok(TaskOutcome::Failed(
                    TaskError::ResetDatasetError(ResetDatasetTaskError::ResetHeadNotFound),
                )),
                _ => Ok(TaskOutcome::Failed(TaskError::Empty)),
            },
        }
    }

    async fn run_hard_compaction(
        &self,
        args: &HardCompactionDataset,
    ) -> Result<TaskOutcome, InternalError> {
        let compaction_svc = self.catalog.get_one::<dyn CompactionService>().int_err()?;
        let dataset_repo = self.catalog.get_one::<dyn DatasetRepository>().int_err()?;
        let dataset_handle = dataset_repo
            .resolve_dataset_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_result = compaction_svc
            .compact_dataset(
                &dataset_handle,
                CompactionOptions {
                    max_slice_size: args.max_slice_size,
                    max_slice_records: args.max_slice_records,
                    keep_metadata_only: args.keep_metadata_only,
                },
                None,
            )
            .await;

        match compaction_result {
            Ok(result) => Ok(TaskOutcome::Success(TaskResult::CompactionDatasetResult(
                result.into(),
            ))),
            Err(_) => Ok(TaskOutcome::Failed(TaskError::Empty)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskLogicalPlanRunner for TaskLogicalPlanRunnerImpl {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn run_plan(&self, logical_plan: &LogicalPlan) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(?logical_plan, "Running task plan");

        let task_outcome = match logical_plan {
            LogicalPlan::UpdateDataset(upd) => self.run_update(upd).await?,
            LogicalPlan::Probe(probe) => self.run_probe(probe).await?,
            LogicalPlan::Reset(reset) => self.run_reset(reset).await?,
            LogicalPlan::HardCompactionDataset(compaction) => {
                self.run_hard_compaction(compaction).await?
            }
        };

        Ok(task_outcome)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
