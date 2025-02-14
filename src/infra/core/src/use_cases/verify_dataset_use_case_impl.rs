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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{
    DatasetRegistry,
    VerificationListener,
    VerificationMultiListener,
    VerificationRequest,
    VerificationResult,
    VerificationService,
    VerifyDatasetUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn VerifyDatasetUseCase)]
pub struct VerifyDatasetUseCaseImpl {
    verification_service: Arc<dyn VerificationService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl VerifyDatasetUseCaseImpl {
    pub fn new(
        verification_service: Arc<dyn VerificationService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            verification_service,
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl VerifyDatasetUseCase for VerifyDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "VerifyDatasetUseCase::execute",
        skip_all,
        fields(?request)
    )]
    async fn execute(
        &self,
        request: VerificationRequest<odf::DatasetHandle>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult {
        // Permission check
        // TODO: verification of derived datasets requires read permission for inputs
        match self
            .dataset_action_authorizer
            .check_action_allowed(&request.target.id, DatasetAction::Read)
            .await
        {
            Ok(_) => {}
            Err(e) => return VerificationResult::err(request.target.clone(), e),
        };

        // Resolve dataset
        let target = self
            .dataset_registry
            .get_dataset_by_handle(&request.target)
            .await;

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
        name = "VerifyDatasetUseCase::execute_multi",
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
            let res = self
                .dataset_action_authorizer
                .check_action_allowed(&request.target.id, DatasetAction::Read)
                .await;
            match res {
                Ok(_) => authorized_requests.push(VerificationRequest {
                    target: self
                        .dataset_registry
                        .get_dataset_by_handle(&request.target)
                        .await,
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
