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
use kamu_auth_rebac::{RebacDatasetIdUnresolvedError, RebacDatasetRegistryFacade};
use kamu_core::{
    auth,
    VerificationError,
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
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

impl VerifyDatasetUseCaseImpl {
    pub fn new(
        verification_service: Arc<dyn VerificationService>,
        rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    ) -> Self {
        Self {
            verification_service,
            rebac_dataset_registry_facade,
        }
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
        // TODO: Private Datasets: verification of derived datasets requires read
        //       permission for inputs

        // Resolve dataset
        let resolve_result = self
            .rebac_dataset_registry_facade
            .resolve_dataset_by_handle(&request.target, auth::DatasetAction::Read)
            .await
            .map_err(map_unresolved_error);
        let target = match resolve_result {
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
            let res = self
                .rebac_dataset_registry_facade
                .resolve_dataset_by_handle(&request.target, auth::DatasetAction::Read)
                .await
                .map_err(map_unresolved_error);
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

fn map_unresolved_error(e: RebacDatasetIdUnresolvedError) -> VerificationError {
    use RebacDatasetIdUnresolvedError as E;
    match e {
        E::Access(e) => VerificationError::Access(e),
        e @ E::Internal(_) => VerificationError::Internal(e.int_err()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
