// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ErrorIntoInternal;
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::{
    GetDataOptions,
    GetDataResponse,
    QueryDatasetDataUseCase,
    QueryError,
    QueryService,
};
use kamu_datasets::{DatasetAction, DatasetRegistry, ResolvedDataset};

use super::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn QueryDatasetDataUseCase)]
pub struct QueryDatasetDataUseCaseImpl {
    query_service: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl QueryDatasetDataUseCaseImpl {
    async fn resolve_multiple_datasets(
        &self,
        dataset_refs: &[odf::DatasetRef],
        skip_if_missing_or_inaccessible: bool,
    ) -> Result<Vec<ResolvedDataset>, QueryError> {
        let refs: Vec<&odf::DatasetRef> = dataset_refs.iter().collect();
        let classified = self
            .rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(&refs, DatasetAction::Read)
            .await?;

        if !skip_if_missing_or_inaccessible
            && let Some((_, err)) = classified.inaccessible_refs.into_iter().next()
        {
            use kamu_auth_rebac::RebacDatasetRefUnresolvedError as E;
            let err = match err {
                E::NotFound(e) => QueryError::DatasetNotFound(e),
                E::Access(e) => QueryError::Access(e),
                e @ E::Internal(_) => QueryError::Internal(e.int_err()),
            };
            return Err(err);
        }

        let mut resolved_datasets = Vec::with_capacity(classified.accessible_resolved_refs.len());
        for (_, hdl) in classified.accessible_resolved_refs {
            let resolved = self.dataset_registry.get_dataset_by_handle(&hdl).await;
            resolved_datasets.push(resolved);
        }

        Ok(resolved_datasets)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl QueryDatasetDataUseCase for QueryDatasetDataUseCaseImpl {
    #[tracing::instrument(level = "debug", name = QueryDatasetDataUseCaseImpl_tail, skip_all, fields(%dataset_ref))]
    async fn tail(
        &self,
        dataset_ref: &odf::DatasetRef,
        skip: u64,
        limit: u64,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        // Resolve source dataset with ReBAC check
        let source = helpers::resolve_dataset_for_querying(
            self.rebac_dataset_registry_facade.as_ref(),
            dataset_ref,
        )
        .await?;

        // Perform tail query
        self.query_service.tail(source, skip, limit, options).await
    }

    #[tracing::instrument(level = "debug", name = QueryDatasetDataUseCaseImpl_get_data, skip_all, fields(%dataset_ref))]
    async fn get_data(
        &self,
        dataset_ref: &odf::DatasetRef,
        options: GetDataOptions,
    ) -> Result<GetDataResponse, QueryError> {
        // Resolve source dataset with ReBAC check
        let source = helpers::resolve_dataset_for_querying(
            self.rebac_dataset_registry_facade.as_ref(),
            dataset_ref,
        )
        .await?;

        // Perform get data query
        self.query_service.get_data(source, options).await
    }

    // TODO: Consider replacing this function with a more sophisticated session
    // context builder that can be reused for multiple queries
    /// Returns [`DataFrameExt`]s representing the contents of multiple datasets
    /// in a batch
    #[tracing::instrument(level = "debug", name = QueryDatasetDataUseCaseImpl_get_data_multi, skip_all)]
    async fn get_data_multi(
        &self,
        dataset_refs: &[odf::DatasetRef],
        skip_if_missing_or_inaccessible: bool,
    ) -> Result<Vec<GetDataResponse>, QueryError> {
        // Resolve source datasets with ReBAC check
        let sources = self
            .resolve_multiple_datasets(dataset_refs, skip_if_missing_or_inaccessible)
            .await?;

        // Perform get data multi query
        self.query_service.get_data_multi(sources).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
