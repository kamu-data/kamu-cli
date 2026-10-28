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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use kamu_datasets::{
    ClassifyByAllowanceResponse,
    DatasetAction,
    DatasetActionAuthorizer,
    DatasetRegistry,
    ResolvedDataset,
};
use random_strings::get_random_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs one transform job of `kamu pull`: elaborates, resets the derivative
/// to metadata on a diverged input when asked, authorizes inputs the plan did
/// not include, then executes and commits
pub(crate) struct PullTransformJob {
    update_steps: Arc<dyn DatasetUpdateSteps>,
    transform_executor: Arc<dyn TransformExecutor>,
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    catalog: dill::Catalog,
}

impl PullTransformJob {
    pub(crate) fn new(
        update_steps: Arc<dyn DatasetUpdateSteps>,
        transform_executor: Arc<dyn TransformExecutor>,
        compaction_planner: Arc<dyn CompactionPlanner>,
        compaction_executor: Arc<dyn CompactionExecutor>,
        catalog: dill::Catalog,
    ) -> Self {
        Self {
            update_steps,
            transform_executor,
            compaction_planner,
            compaction_executor,
            catalog,
        }
    }

    pub(crate) async fn run(
        self,
        pti: PullTransformItem,
        transform_options: TransformOptions,
        maybe_listener: Option<Arc<dyn TransformListener>>,
    ) -> Result<PullResponse, InternalError> {
        let maybe_original_request = pti.maybe_original_request.clone();
        let target_handle = pti.target.get_handle().clone();
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullTransformListener));

        let transform_result = self
            .elaborate_and_execute(pti, transform_options, listener.clone())
            .await;

        match &transform_result {
            Ok(result) => listener.success(result),
            Err(e) => listener.error(e),
        }

        Ok(PullResponse {
            maybe_original_request,
            maybe_local_ref: Some(target_handle.as_local_ref()),
            maybe_remote_ref: None,
            result: transform_result.map(Into::into),
        })
    }

    async fn elaborate_and_execute(
        &self,
        pti: PullTransformItem,
        transform_options: TransformOptions,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, PullError> {
        let target_handle = pti.target.get_handle();
        let mut maybe_reset_heads = None;

        let elaboration = match self.update_steps.elaborate_transform(target_handle).await {
            Err(TransformElaborateError::InvalidInputInterval(e))
                if transform_options.reset_derivatives_on_diverged_input =>
            {
                tracing::warn!(error = %e, "Input diverged, resetting the derivative to metadata");

                match self.reset_to_metadata(target_handle).await? {
                    Some(reset_heads) => {
                        maybe_reset_heads = Some(reset_heads);
                        self.update_steps.elaborate_transform(target_handle).await
                    }
                    None => Err(TransformElaborateError::InvalidInputInterval(e)),
                }
            }
            result => result,
        };

        let result = self
            .execute_elaborated(elaboration, target_handle, &pti.input_handles, listener)
            .await;

        match (maybe_reset_heads, result) {
            (maybe_reset_heads, Ok(transform_result)) => Ok(Self::combine_with_reset(
                maybe_reset_heads,
                transform_result,
            )),
            (Some((old_head, new_head)), Err(source)) => Err(PullError::TransformStopped(
                Box::new(TransformStoppedError {
                    old_head,
                    new_head,
                    source,
                }),
            )),
            (None, Err(e)) => Err(e),
        }
    }

    /// Authorizes, executes and commits an elaborated transform
    async fn execute_elaborated(
        &self,
        elaboration: Result<Option<ElaboratedTransform>, TransformElaborateError>,
        target_handle: &odf::DatasetHandle,
        planned_inputs: &[odf::DatasetHandle],
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, PullError> {
        match elaboration {
            Ok(Some(ElaboratedTransform {
                target,
                elaboration: TransformElaboration::Elaborated(plan),
            })) => {
                self.authorize_unplanned_inputs(&plan, planned_inputs)
                    .await?;
                self.execute_and_commit(target, plan, listener).await
            }
            Ok(Some(ElaboratedTransform {
                elaboration: TransformElaboration::UpToDate,
                ..
            })) => Ok(TransformResult::UpToDate),
            Ok(None) => Err(PullError::NotFound(odf::DatasetNotFoundError::new(
                target_handle.as_local_ref(),
            ))),
            Err(e) => Err(TransformError::from(e).into()),
        }
    }

    /// The pull authorized reading the inputs of the transform it planned.
    /// Another writer may change the transform before the job elaborates it,
    /// so inputs the pull did not plan are authorized here, before any is read
    #[transactional_method1(dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>)]
    async fn authorize_unplanned_inputs(
        &self,
        plan: &TransformExecutionPlan,
        planned_inputs: &[odf::DatasetHandle],
    ) -> Result<(), PullError> {
        let unplanned_inputs: Vec<_> = plan
            .request
            .inputs
            .iter()
            .map(|input| &input.dataset_handle)
            .filter(|handle| !planned_inputs.iter().any(|planned| planned.id == handle.id))
            .cloned()
            .collect();
        if unplanned_inputs.is_empty() {
            return Ok(());
        }

        let ClassifyByAllowanceResponse {
            authorized_handles: _,
            unauthorized_handles_with_errors,
        } = dataset_action_authorizer
            .classify_dataset_handles_by_allowance(unplanned_inputs, DatasetAction::Read)
            .await?;

        match unauthorized_handles_with_errors.into_iter().next() {
            None => Ok(()),
            Some((_, error)) => Err(error.into()),
        }
    }

    async fn execute_and_commit(
        &self,
        target: ResolvedDataset,
        plan: TransformExecutionPlan,
        listener: Arc<dyn TransformListener>,
    ) -> Result<TransformResult, PullError> {
        let (target, result) = self
            .transform_executor
            .execute_transform(target, plan, &get_random_name(None, 10), Some(listener))
            .await;

        let result = result.map_err(TransformError::from)?;
        if let TransformResult::Updated {
            old_head, new_head, ..
        } = &result
        {
            self.commit_head(target.get_handle(), old_head, new_head)
                .await?;
        }

        Ok(result)
    }

    async fn commit_head(
        &self,
        target_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), PullError> {
        HeadCommit::into_pull_result(
            self.update_steps
                .commit_head(target_handle, old_head, new_head)
                .await,
            target_handle,
        )
    }

    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn plan_reset_to_metadata(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<(ResolvedDataset, CompactionPlan), PullError> {
        let target = dataset_registry.get_dataset_by_handle(target_handle).await;
        let plan = self
            .compaction_planner
            .plan_compaction(
                target.clone(),
                CompactionOptions {
                    keep_metadata_only: true,
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(|e| match e {
                CompactionPlanningError::Access(e) => PullError::Access(e),
                e @ (CompactionPlanningError::InvalidDatasetKind(_)
                | CompactionPlanningError::Internal(_)) => PullError::Internal(e.int_err()),
            })?;

        target.detach_from_transaction();
        Ok((target, plan))
    }

    /// Resets the derivative to metadata, returning the heads before and
    /// after, or `None` when there was nothing to reset.
    async fn reset_to_metadata(
        &self,
        target_handle: &odf::DatasetHandle,
    ) -> Result<Option<(odf::Multihash, odf::Multihash)>, PullError> {
        let (target, plan) = self.plan_reset_to_metadata(target_handle).await?;

        match self
            .compaction_executor
            .execute(target, plan, None)
            .await
            .int_err()?
        {
            CompactionResult::NothingToDo => Ok(None),
            CompactionResult::Success {
                old_head, new_head, ..
            } => {
                self.commit_head(target_handle, &old_head, &new_head)
                    .await?;
                Ok(Some((old_head, new_head)))
            }
        }
    }

    /// A reset and the transform after it are reported as one update. The
    /// offsets and the increment stay the transform's own: the reset rewrote
    /// history, so no interval spans both
    fn combine_with_reset(
        maybe_reset_heads: Option<(odf::Multihash, odf::Multihash)>,
        transform_result: TransformResult,
    ) -> TransformResult {
        match (maybe_reset_heads, transform_result) {
            (
                Some((old_head, _)),
                TransformResult::Updated {
                    new_head,
                    new_offset_interval,
                    increment,
                    ..
                },
            ) => TransformResult::Updated {
                old_head,
                new_head,
                new_offset_interval,
                increment,
            },
            (Some((old_head, new_head)), TransformResult::UpToDate) => TransformResult::Updated {
                old_head,
                new_head,
                new_offset_interval: None,
                increment: odf::dataset::MetadataChainIncrementInterval::default(),
            },
            (None, transform_result) => transform_result,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
