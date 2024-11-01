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

use database_common_macros::{transactional_method1, transactional_method2};
use dill::*;
use internal_error::InternalError;
use kamu_core::*;
use kamu_datasets::{DatasetEnvVar, DatasetEnvVarService};
use kamu_task_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskLogicalPlanRunnerImpl {
    catalog: Catalog,
    in_multi_tenant_mode: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn TaskLogicalPlanRunner)]
impl TaskLogicalPlanRunnerImpl {
    pub fn new(catalog: Catalog, in_multi_tenant_mode: bool) -> Self {
        Self {
            catalog,
            in_multi_tenant_mode,
        }
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
        // Prepare task definition: requires a transaction
        let task_definition = self.prepare_update_task_definition(args).await?;
        tracing::info!(task_definition = ?task_definition, "Built update task definition");

        // Run update task, this does not require a transaction
        match task_definition {
            UpdateTaskDefinition::Pull(task) => match task.pull_job {
                PullPlanIterationJob::Ingest(mut ingest_batch) => {
                    assert_eq!(ingest_batch.len(), 1);
                    let ingest_item = ingest_batch.remove(0);
                    self.run_ingest_update(ingest_item, task.pull_options.ingest_options)
                        .await
                }
                PullPlanIterationJob::Transform(mut transform_batch) => {
                    assert_eq!(transform_batch.len(), 1);
                    let transform_item = transform_batch.remove(0);
                    self.run_transform_update(transform_item).await
                }
                PullPlanIterationJob::Sync(_) => {
                    unreachable!("No Sync jobs possible from update requests");
                }
            },
            UpdateTaskDefinition::Result(outcome) => Ok(outcome),
        }
    }

    async fn run_ingest_update(
        &self,
        ingest_item: PullIngestItem,
        ingest_options: PollingIngestOptions,
    ) -> Result<TaskOutcome, InternalError> {
        let polling_ingest_service = self.catalog.get_one::<dyn PollingIngestService>().unwrap();
        match polling_ingest_service
            .ingest(ingest_item.target, ingest_options, None)
            .await
        {
            Ok(ingest_result) => Ok(TaskOutcome::Success(TaskResult::UpdateDatasetResult(
                TaskUpdateDatasetResult {
                    pull_result: ingest_result.into(),
                },
            ))),
            Err(_) => Ok(TaskOutcome::Failed(TaskError::Empty)),
        }
    }

    async fn run_transform_update(
        &self,
        transform_item: PullTransformItem,
    ) -> Result<TaskOutcome, InternalError> {
        let transform_execution_service = self
            .catalog
            .get_one::<dyn TransformExecutionService>()
            .unwrap();

        match transform_item.plan {
            TransformPlan::ReadyToLaunch(operation) => {
                let (_, execution_result) = transform_execution_service
                    .execute_transform(transform_item.target, operation, None)
                    .await;

                match execution_result {
                    Ok(transform_result) => Ok(TaskOutcome::Success(
                        TaskResult::UpdateDatasetResult(TaskUpdateDatasetResult {
                            pull_result: transform_result.into(),
                        }),
                    )),
                    Err(e) => {
                        tracing::error!(error = ?e, "Transform execution failed");
                        Ok(TaskOutcome::Failed(TaskError::Empty))
                    }
                }
            }
            TransformPlan::UpToDate => Ok(TaskOutcome::Success(TaskResult::Empty)),
        }
    }

    #[transactional_method2(
        dataset_env_vars_svc: Arc<dyn DatasetEnvVarService>,
        pull_request_planner: Arc<dyn PullRequestPlanner>
    )]
    async fn prepare_update_task_definition(
        &self,
        args: &UpdateDataset,
    ) -> Result<UpdateTaskDefinition, InternalError> {
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
                self.in_multi_tenant_mode,
            )
            .await;

        match plan_res {
            Ok(pull_job) => Ok(UpdateTaskDefinition::Pull(UpdateTaskDefinitionPull {
                pull_options,
                pull_job,
            })),
            Err(e) => {
                assert!(e.result.is_err());
                // Special case: input dataset compacted
                if let Err(PullError::TransformPlanError(
                    TransformPlanError::InvalidInputInterval(e),
                )) = e.result
                {
                    Ok(UpdateTaskDefinition::Result(TaskOutcome::Failed(
                        TaskError::UpdateDatasetError(
                            UpdateDatasetTaskError::InputDatasetCompacted(
                                InputDatasetCompactedError {
                                    dataset_id: e.input_dataset_id,
                                },
                            ),
                        ),
                    )))
                } else {
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

    async fn run_reset(&self, args: &ResetDataset) -> Result<TaskOutcome, InternalError> {
        // Prepare task definition: requires a transaction
        let task_definition = self.prepare_reset_task_definition(args).await?;
        tracing::info!(task_definition = ?task_definition, "Built reset task definition");

        // Run task: this does not require transaction
        let reset_svc = self.catalog.get_one::<dyn ResetService>().int_err()?;
        let reset_result_maybe = reset_svc
            .reset_dataset(
                task_definition.target.dataset,
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
                err => {
                    tracing::error!(
                        args = ?args,
                        error = ?err,
                        error_msg = %err,
                        "Reset failed",
                    );

                    Ok(TaskOutcome::Failed(TaskError::Empty))
                }
            },
        }
    }

    #[transactional_method1(
        dataset_registry: Arc<dyn DatasetRegistry>
    )]
    async fn prepare_reset_task_definition(
        &self,
        args: &ResetDataset,
    ) -> Result<ResetTaskDefinition, InternalError> {
        let target = dataset_registry
            .get_resolved_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        Ok(ResetTaskDefinition { target })
    }

    async fn run_hard_compaction(
        &self,
        args: &HardCompactionDataset,
    ) -> Result<TaskOutcome, InternalError> {
        // Prepare task definition: requires a transaction
        let task_definition = self.prepare_hard_compaction_task_definition(args).await?;
        tracing::info!(task_definition = ?task_definition, "Built hard compaction task definition");

        // Run task: this does not require transaction
        let compaction_svc = self.catalog.get_one::<dyn CompactionService>().int_err()?;

        let compaction_result = compaction_svc
            .compact_dataset(
                task_definition.target,
                task_definition.compaction_options,
                None,
            )
            .await;

        match compaction_result {
            Ok(result) => Ok(TaskOutcome::Success(TaskResult::CompactionDatasetResult(
                result.into(),
            ))),
            Err(err) => {
                tracing::error!(
                    args = ?args,
                    error = ?err,
                    error_msg = %err,
                    "Hard compaction failed",
                );

                Ok(TaskOutcome::Failed(TaskError::Empty))
            }
        }
    }

    #[transactional_method1(
        dataset_registry: Arc<dyn DatasetRegistry>
    )]
    async fn prepare_hard_compaction_task_definition(
        &self,
        args: &HardCompactionDataset,
    ) -> Result<HardCompactionTaskDefinition, InternalError> {
        let target = dataset_registry
            .get_resolved_dataset_by_ref(&args.dataset_id.as_local_ref())
            .await
            .int_err()?;

        let compaction_options = CompactionOptions {
            max_slice_size: args.max_slice_size,
            max_slice_records: args.max_slice_records,
            keep_metadata_only: args.keep_metadata_only,
        };

        Ok(HardCompactionTaskDefinition {
            target,
            compaction_options,
        })
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

#[derive(Debug)]
enum UpdateTaskDefinition {
    Pull(UpdateTaskDefinitionPull),
    Result(TaskOutcome),
}

#[derive(Debug)]
struct UpdateTaskDefinitionPull {
    pull_options: PullOptions,
    pull_job: PullPlanIterationJob,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct ResetTaskDefinition {
    target: ResolvedDataset,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct HardCompactionTaskDefinition {
    target: ResolvedDataset,
    compaction_options: CompactionOptions,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
