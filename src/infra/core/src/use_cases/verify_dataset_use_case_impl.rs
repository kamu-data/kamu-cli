// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    TransformPlanner,
    VerificationError,
    VerificationListener,
    VerificationMultiListener,
    VerificationRequest,
    VerificationResult,
    VerificationService,
    VerifyDatasetUseCase,
    VerifyTransformError,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetRegistry,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn VerifyDatasetUseCase)]
pub struct VerifyDatasetUseCaseImpl {
    verification_service: Arc<dyn VerificationService>,
    dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    transform_planner: Arc<dyn TransformPlanner>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

impl VerifyDatasetUseCaseImpl {
    /// Checks the target before its chain is read, then the inputs a replay
    /// of its transforms would read
    async fn authorize(
        &self,
        request: &VerificationRequest<odf::DatasetHandle>,
    ) -> Result<ResolvedDataset, VerificationError> {
        self.authorize_verify(&request.target, Vec::new()).await?;

        let target = self
            .dataset_registry
            .get_dataset_by_handle(&request.target)
            .await;

        if request.options.replay_transformations
            && target.get_kind() == odf::DatasetKind::Derivative
        {
            let replayed_inputs = self
                .transform_planner
                .get_verification_inputs(target.clone(), request.block_range.clone())
                .await
                .map_err(|e| VerificationError::VerifyTransform(VerifyTransformError::Plan(e)))?;

            self.authorize_verify(&request.target, replayed_inputs)
                .await?;
        }

        Ok(target)
    }

    async fn authorize_verify(
        &self,
        target: &odf::DatasetHandle,
        replayed_inputs: Vec<odf::DatasetHandle>,
    ) -> Result<(), VerificationError> {
        self.dataset_operation_guard
            .check_operation(
                &self.current_account_subject,
                &DatasetOperationRequest::new(
                    target.clone(),
                    DatasetOperation::Verify { replayed_inputs },
                ),
            )
            .await
            .map_err(map_unauthorized_error)
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl VerifyDatasetUseCase for VerifyDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = VerifyDatasetUseCaseImpl_execute,
        skip_all,
        fields(?request)
    )]
    async fn execute(
        &self,
        request: VerificationRequest<odf::DatasetHandle>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult {
        let target = match self.authorize(&request).await {
            Ok(target) => target,
            Err(e) => return VerificationResult::err(request.target.clone(), e),
        };

        // Actual action
        self.verification_service
            .verify(
                VerificationRequest {
                    target,
                    block_range: request.block_range,
                    options: request.options,
                },
                maybe_listener,
            )
            .await
    }

    #[tracing::instrument(
        level = "info",
        name = VerifyDatasetUseCaseImpl_execute_multi,
        skip_all,
        fields(?requests)
    )]
    async fn execute_multi(
        &self,
        requests: Vec<VerificationRequest<odf::DatasetHandle>>,
        maybe_multi_listener: Option<Arc<dyn VerificationMultiListener>>,
    ) -> Vec<VerificationResult> {
        let mut verification_results = Vec::new();

        // Exclude tasks, where there is no read permission for dataset
        let mut authorized_requests = Vec::new();
        for request in requests {
            let res = self.authorize(&request).await;
            match res {
                Ok(resolved_dataset) => authorized_requests.push(VerificationRequest {
                    target: resolved_dataset,
                    block_range: request.block_range,
                    options: request.options,
                }),
                Err(e) => {
                    verification_results.push(VerificationResult::err(request.target, e));
                }
            }
        }

        // Run verification for authorized datasets
        let mut authorized_results = self
            .verification_service
            .verify_multi(authorized_requests, maybe_multi_listener)
            .await;

        // Join results
        verification_results.append(&mut authorized_results);
        verification_results
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn map_unauthorized_error(e: DatasetPermissionError) -> VerificationError {
    use DatasetPermissionError as E;

    match e {
        E::NotFound(e) => VerificationError::DatasetNotFound(e),
        E::Access(e) => VerificationError::Access(e),
        e @ E::Internal(_) => VerificationError::Internal(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
