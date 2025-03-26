// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_auth_rebac::{
    ClassifyDatasetRefsByAllowanceResponse,
    RebacDatasetIdUnresolvedError,
    RebacDatasetRefUnresolvedError,
    RebacDatasetRegistryFacade,
};
use kamu_core::{auth, DatasetRegistry, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RebacDatasetRegistryFacadeImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
}

#[dill::component(pub)]
#[dill::interface(dyn RebacDatasetRegistryFacade)]
impl RebacDatasetRegistryFacadeImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl RebacDatasetRegistryFacade for RebacDatasetRegistryFacadeImpl {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: auth::DatasetAction,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError> {
        use RebacDatasetRefUnresolvedError as Error;

        let handle = self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
            .map_err(|e| {
                use odf::DatasetRefUnresolvedError as SourceError;

                match e {
                    SourceError::NotFound(e) => Error::NotFound(e),
                    e @ SourceError::Internal(_) => Error::Internal(e.int_err()),
                }
            })?;

        self.dataset_action_authorizer
            .check_action_allowed(&handle.id, action)
            .await
            .map_err(|e| {
                use auth::DatasetActionUnauthorizedError as SourceError;

                match e {
                    SourceError::Access(e) => Error::Access(e),
                    e @ SourceError::Internal(_) => Error::Internal(e.int_err()),
                }
            })?;

        Ok(handle)
    }

    async fn resolve_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetRefUnresolvedError> {
        let dataset_handle = self
            .resolve_dataset_handle_by_ref(dataset_ref, action)
            .await?;
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(&dataset_handle)
            .await?;

        Ok(resolved_dataset)
    }

    async fn resolve_dataset_by_handle(
        &self,
        dataset_handle: &odf::DatasetHandle,
        action: auth::DatasetAction,
    ) -> Result<ResolvedDataset, RebacDatasetIdUnresolvedError> {
        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, action)
            .await
            .map_err(|e| {
                use auth::DatasetActionUnauthorizedError as SourceError;
                use RebacDatasetIdUnresolvedError as Error;

                match e {
                    SourceError::Access(e) => Error::Access(e),
                    e @ SourceError::Internal(_) => Error::Internal(e.int_err()),
                }
            })?;
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await?;

        Ok(resolved_dataset)
    }

    async fn classify_dataset_refs_by_allowance(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
        action: auth::DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAllowanceResponse, InternalError> {
        let mut res = ClassifyDatasetRefsByAllowanceResponse {
            accessible_resolved_refs: Vec::with_capacity(dataset_refs.len()),
            inaccessible_refs: vec![],
        };

        // TODO: Private Datasets:
        // TODO: PERF: resolve multi refs at once
        for dataset_ref in dataset_refs {
            match self
                .resolve_dataset_handle_by_ref(&dataset_ref, action)
                .await
            {
                Ok(handle) => {
                    res.accessible_resolved_refs.push((dataset_ref, handle));
                }
                Err(e) => {
                    use RebacDatasetRefUnresolvedError as E;
                    match e {
                        e @ (E::NotFound(_) | E::Access(_)) => {
                            res.inaccessible_refs.push((dataset_ref, e));
                        }
                        e @ E::Internal(_) => return Err(e.int_err()),
                    }
                }
            }
        }

        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
