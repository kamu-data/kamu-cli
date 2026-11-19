// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use dill::{component, interface};
use kamu_accounts::Principal;
use kamu_core::{
    NullVerificationListener,
    TransformPlanner,
    VerificationError,
    VerificationListener,
    VerificationPlan,
    VerificationRequest,
    VerificationResult,
    VerificationService,
    VerifyJob,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetRegistry,
};

use super::job_target::resolve_job_target;
use crate::utils::span_outcome::SpanOutcome;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn VerifyJob)]
pub struct VerifyJobImpl {
    catalog: dill::Catalog,
    verification_service: Arc<dyn VerificationService>,
    principal: Arc<Principal>,
}

impl VerifyJobImpl {
    /// Authorizes reading the target, then the inputs its replay reads, before
    /// any of them is read
    async fn authorize_and_plan(
        &self,
        dataset_operation_guard: &dyn DatasetOperationGuard,
        dataset_registry: &dyn DatasetRegistry,
        transform_planner: &dyn TransformPlanner,
        request: &VerificationRequest<odf::DatasetHandle>,
    ) -> Result<VerificationPlan, VerificationError> {
        self.authorize(dataset_operation_guard, &request.target, Vec::new())
            .await?;

        let target =
            resolve_job_target::<VerificationError>(dataset_registry, &request.target).await?;
        let request = VerificationRequest {
            target,
            block_range: request.block_range.clone(),
            options: request.options.clone(),
        };

        if VerificationPlan::replays_transforms(&request) {
            match transform_planner
                .get_verification_inputs(request.target.clone(), request.block_range.clone())
                .await
            {
                Ok(replayed_inputs) => {
                    self.authorize(
                        dataset_operation_guard,
                        request.target.get_handle(),
                        replayed_inputs,
                    )
                    .await?;
                }
                // A replay that cannot be planned reads no input, so none is authorized
                Err(e) => return VerificationPlan::with_unplanned_replay(request, e).await,
            }
        }

        VerificationPlan::build(request, transform_planner).await
    }

    fn span_outcome(outcome: &Result<(), VerificationError>) -> SpanOutcome {
        match outcome {
            Ok(()) => SpanOutcome::Verified,
            Err(VerificationError::DatasetNotFound(_)) => SpanOutcome::DatasetDeleted,
            Err(VerificationError::Access(_)) => SpanOutcome::NotAllowed,
            Err(
                VerificationError::RefNotFound(_)
                | VerificationError::BlockNotFound(_)
                | VerificationError::BlockVersion(_)
                | VerificationError::BlockMalformed(_)
                | VerificationError::InvalidInterval(_)
                | VerificationError::DataDoesNotMatchMetadata(_)
                | VerificationError::CheckpointDoesNotMatchMetadata(_)
                | VerificationError::VerifyTransform(_)
                | VerificationError::Internal(_),
            ) => SpanOutcome::Failed,
        }
    }

    async fn authorize(
        &self,
        dataset_operation_guard: &dyn DatasetOperationGuard,
        target: &odf::DatasetHandle,
        replayed_inputs: Vec<odf::DatasetHandle>,
    ) -> Result<(), VerificationError> {
        dataset_operation_guard
            .check_operation(
                &self.principal,
                &DatasetOperationRequest::new(
                    target.clone(),
                    DatasetOperation::Verify { replayed_inputs },
                ),
            )
            .await?;
        Ok(())
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl VerifyJob for VerifyJobImpl {
    #[tracing::instrument(level = "info", name = VerifyJobImpl_run, skip_all, fields(target = %request.target, block_range = ?request.block_range, outcome))]
    async fn run(
        &self,
        request: VerificationRequest<odf::DatasetHandle>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult {
        let plan = transaction!(|dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                                 dataset_registry: Arc<dyn DatasetRegistry>,
                                 transform_planner: Arc<dyn TransformPlanner>|
         -> Result<VerificationPlan, VerificationError> {
            self.authorize_and_plan(
                dataset_operation_guard.as_ref(),
                dataset_registry.as_ref(),
                transform_planner.as_ref(),
                &request,
            )
            .await
        });

        let result = match plan {
            // Integrity checks and the replay read storage only
            Ok(plan) => self.verification_service.verify(plan, maybe_listener).await,
            Err(e) => {
                let listener =
                    maybe_listener.unwrap_or_else(|| Arc::new(NullVerificationListener {}));
                listener.begin();
                listener.error(&e);
                VerificationResult::err(request.target, e)
            }
        };
        Self::span_outcome(&result.outcome).record();
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
