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

use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_auth_rebac::{
    ClassifyDatasetRefsByAccessResponse,
    ClassifyDatasetRefsByAllowanceResponse,
    RebacDatasetIdUnresolvedError,
    RebacDatasetRefUnresolvedError,
    RebacDatasetRegistryFacade,
};
use kamu_datasets::{
    ClassifyByAllowanceDatasetPermissionError,
    DatasetPermission,
    DatasetPermissionAccess,
    DatasetPermissionChecker,
    DatasetPermissionError,
    DatasetRegistry,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn RebacDatasetRegistryFacade)]
pub struct RebacDatasetRegistryFacadeImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_permission_checker: Arc<dyn DatasetPermissionChecker>,
}

#[async_trait::async_trait]
impl RebacDatasetRegistryFacade for RebacDatasetRegistryFacadeImpl {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: DatasetPermission,
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

        self.dataset_permission_checker
            .check_permission(&handle.id, action)
            .await
            .map_err(|e| {
                use DatasetPermissionError as SourceError;

                match e {
                    SourceError::NotFound(e) => Error::NotFound(e),
                    SourceError::Access(e) => Error::Access(e),
                    e @ SourceError::Internal(_) => Error::Internal(e.int_err()),
                }
            })?;

        Ok(handle)
    }

    async fn resolve_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: DatasetPermission,
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
        action: DatasetPermission,
    ) -> Result<ResolvedDataset, RebacDatasetIdUnresolvedError> {
        self.dataset_permission_checker
            .check_permission(&dataset_handle.id, action)
            .await
            .map_err(|e| {
                use DatasetPermissionError as SourceError;
                use RebacDatasetIdUnresolvedError as Error;

                match e {
                    SourceError::NotFound(e) => Error::NotFound(e),
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
        action: DatasetPermission,
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

        // Build mapping of resolved handles to ref
        let mut resolved_handles_to_refs = handles_resolution
            .resolved_handles
            .into_iter()
            .map(|(dataset_ref, handle)| (handle, dataset_ref))
            .collect::<HashMap<_, _>>();

        // Extract handles for action classification
        let resolved_handles = resolved_handles_to_refs.keys().cloned().collect();

        // Run vectorized classification
        let classification = self
            .dataset_permission_checker
            .classify_dataset_handles_by_allowance(resolved_handles, action)
            .await?;

        // Collect authorized handles
        for handle in classification.authorized_handles {
            let handle_ref = resolved_handles_to_refs.remove(&handle).unwrap();
            accessible_resolved_refs.push((handle_ref, handle));
        }

        // Create errors for unauthorized handles
        for (handle, error) in classification.unauthorized_handles_with_errors {
            let handle_ref = resolved_handles_to_refs.remove(&handle).unwrap();
            let e = match error {
                ClassifyByAllowanceDatasetPermissionError::NotFound(e) => {
                    RebacDatasetRefUnresolvedError::NotFound(e)
                }
                ClassifyByAllowanceDatasetPermissionError::Access(e) => {
                    RebacDatasetRefUnresolvedError::Access(e)
                }
                ClassifyByAllowanceDatasetPermissionError::Internal(e) => {
                    RebacDatasetRefUnresolvedError::Internal(e)
                }
            };

            inaccessible_refs.push((handle_ref, e));
        }

        Ok(ClassifyDatasetRefsByAllowanceResponse {
            accessible_resolved_refs,
            inaccessible_refs,
        })
    }

    async fn classify_dataset_refs_by_access(
        &self,
        dataset_refs: &[&odf::DatasetRef],
        action: DatasetPermission,
    ) -> Result<ClassifyDatasetRefsByAccessResponse, InternalError> {
        use kamu_datasets::DatasetPermissionCheckerExt;

        // The next work will be done using handles, so we need to get them first.
        let handles_resolution = self
            .dataset_registry
            .resolve_dataset_handles_by_refs(dataset_refs)
            .await?;

        let not_found = handles_resolution.unresolved_refs;

        // Parallel execution
        let mut tasks = tokio::task::JoinSet::<Result<_, InternalError>>::new();

        for (dataset_ref, dataset_handle) in handles_resolution.resolved_handles {
            let permission_checker = self.dataset_permission_checker.clone();

            tasks.spawn(async move {
                let allowed_actions = permission_checker
                    .get_granted_permissions_for_existing_dataset(&dataset_handle.id)
                    .await?;
                Ok((
                    dataset_ref,
                    dataset_handle,
                    DatasetPermission::resolve_access(&allowed_actions, action),
                ))
            });
        }

        // Next sort, according to allowed actions
        let mut forbidden = Vec::new();
        let mut insufficient = Vec::new();
        let mut allowed = Vec::new();

        while let Some(join_result) = tasks.join_next().await {
            let result = join_result.int_err()?;
            let (dataset_ref, dataset_handle, access) = result?;

            match access {
                DatasetPermissionAccess::Forbidden => forbidden.push((
                    dataset_ref.clone(),
                    RebacDatasetRefUnresolvedError::not_enough_permissions(dataset_ref, action),
                )),
                DatasetPermissionAccess::Insufficient => {
                    insufficient.push((dataset_ref, dataset_handle));
                }
                DatasetPermissionAccess::Allowed => {
                    allowed.push((dataset_ref, dataset_handle));
                }
            }
        }

        Ok(ClassifyDatasetRefsByAccessResponse {
            not_found,
            forbidden,
            insufficient,
            allowed,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
