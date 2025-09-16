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
    ClassifyDatasetRefsByAccessResponse,
    ClassifyDatasetRefsByAllowanceResponse,
    RebacDatasetIdUnresolvedError,
    RebacDatasetRefUnresolvedError,
    RebacDatasetRegistryFacade,
};
use kamu_core::{DatasetRegistry, ResolvedDataset, auth};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn RebacDatasetRegistryFacade)]
pub struct RebacDatasetRegistryFacadeImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn auth::DatasetActionAuthorizer>,
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
            .await;

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
                use RebacDatasetIdUnresolvedError as Error;
                use auth::DatasetActionUnauthorizedError as SourceError;

                match e {
                    SourceError::Access(e) => Error::Access(e),
                    e @ SourceError::Internal(_) => Error::Internal(e.int_err()),
                }
            })?;
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        Ok(resolved_dataset)
    }

    async fn classify_dataset_refs_by_allowance(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: auth::DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAllowanceResponse, InternalError> {
        // The next work will be done using handles, so we need to get them first.
        let handles_resolution = self
            .dataset_registry
            .resolve_dataset_handles_by_refs(dataset_refs)
            .await?;

        // If no datasets, then access to them cannot exist.
        let mut inaccessible_refs = handles_resolution
            .unresolved_refs
            .into_iter()
            .map(|(dataset_ref, e)| (dataset_ref, e.into()))
            .collect::<Vec<_>>();

        // Next sort according to allowed actions
        let mut accessible_resolved_refs =
            Vec::with_capacity(dataset_refs.len() - inaccessible_refs.len());

        for (dataset_ref, dataset_handle) in handles_resolution.resolved_handles {
            use kamu_core::auth::DatasetActionAuthorizerExt;

            let allowed = self
                .dataset_action_authorizer
                .is_action_allowed(&dataset_handle.id, action)
                .await?;

            if allowed {
                accessible_resolved_refs.push((dataset_ref, dataset_handle));
            } else {
                let e = RebacDatasetRefUnresolvedError::not_enough_permissions(
                    dataset_ref.clone(),
                    action,
                );
                inaccessible_refs.push((dataset_ref, e));
            }
        }

        Ok(ClassifyDatasetRefsByAllowanceResponse {
            accessible_resolved_refs,
            inaccessible_refs,
        })
    }

    async fn classify_dataset_refs_by_access(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: auth::DatasetAction,
    ) -> Result<ClassifyDatasetRefsByAccessResponse, InternalError> {
        // The next work will be done using handles, so we need to get them first.
        let handles_resolution = self
            .dataset_registry
            .resolve_dataset_handles_by_refs(dataset_refs)
            .await?;

        // If no datasets, then access to them cannot exist.
        let mut forbidden = handles_resolution
            .unresolved_refs
            .into_iter()
            .map(|(dataset_ref, e)| (dataset_ref, e.into()))
            .collect::<Vec<_>>();

        // Next sort according to allowed actions
        let mut limited = Vec::new();
        let mut allowed = Vec::new();

        for (dataset_ref, dataset_handle) in handles_resolution.resolved_handles {
            let allowed_actions = self
                .dataset_action_authorizer
                .get_allowed_actions(&dataset_handle.id)
                .await?;

            match auth::DatasetAction::resolve_access(&allowed_actions, action) {
                auth::DatasetActionAccess::Forbidden => forbidden.push((
                    dataset_ref.clone(),
                    RebacDatasetRefUnresolvedError::not_enough_permissions(dataset_ref, action),
                )),
                auth::DatasetActionAccess::Limited => {
                    limited.push((dataset_ref, dataset_handle));
                }
                auth::DatasetActionAccess::Allowed => {
                    allowed.push((dataset_ref, dataset_handle));
                }
            }
        }

        Ok(ClassifyDatasetRefsByAccessResponse {
            forbidden,
            limited,
            allowed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
