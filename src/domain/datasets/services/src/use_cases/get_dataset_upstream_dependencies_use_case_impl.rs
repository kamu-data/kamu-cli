// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use dill::{component, interface};
use internal_error::ResultIntoInternal;
use kamu_core::TenancyConfig;
use kamu_core::auth::{ClassifyByAllowanceIdsResponse, DatasetAction, DatasetActionAuthorizer};
use kamu_datasets::{
    DatasetDependency,
    DatasetEntryService,
    DependencyGraphService,
    GetDatasetUpstreamDependenciesError,
    GetDatasetUpstreamDependenciesUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn GetDatasetUpstreamDependenciesUseCase)]
pub struct GetDatasetUpstreamDependenciesUseCaseImpl {
    tenancy_config: Arc<TenancyConfig>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

#[async_trait::async_trait]
impl GetDatasetUpstreamDependenciesUseCase for GetDatasetUpstreamDependenciesUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "GetDatasetUpstreamDependenciesUseCase::execute",
        skip_all,
        fields(dataset_id)
    )]
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetDependency>, GetDatasetUpstreamDependenciesError> {
        use tokio_stream::StreamExt;

        // TODO: PERF: chunk the stream
        let upstream_dependency_ids = self
            .dependency_graph_service
            .get_upstream_dependencies(dataset_id)
            .await
            .collect::<Vec<_>>()
            .await;
        if upstream_dependency_ids.is_empty() {
            return Ok(Vec::new());
        }

        // NOTE: Borrow-checker can't digest the upfront transformation during stream
        //       data collection. So we need to make an additional vector.
        let upstream_dependency_ids = upstream_dependency_ids
            .into_iter()
            .map(Cow::Owned)
            .collect::<Vec<_>>();
        let mut upstream_dependencies = Vec::with_capacity(upstream_dependency_ids.len());
        let ClassifyByAllowanceIdsResponse {
            authorized_ids,
            unauthorized_ids_with_errors,
        } = self
            .dataset_action_authorizer
            .classify_dataset_ids_by_allowance(&upstream_dependency_ids, DatasetAction::Read)
            .await?;

        upstream_dependencies.extend(unauthorized_ids_with_errors.into_iter().map(
            |(unauthorized_dataset_id, _)| DatasetDependency::Unresolved(unauthorized_dataset_id),
        ));

        let authorized_ids = authorized_ids
            .into_iter()
            .map(Cow::Owned)
            .collect::<Vec<_>>();
        let dataset_entries_resolution = self
            .dataset_entry_service
            .get_multiple_entries(&authorized_ids)
            .await
            .int_err()?;

        for dataset_entry in dataset_entries_resolution.resolved_entries {
            let dataset_alias = self
                .tenancy_config
                .make_alias(dataset_entry.owner_name.clone(), dataset_entry.name);
            let dataset_handle =
                odf::DatasetHandle::new(dataset_entry.id, dataset_alias, dataset_entry.kind);

            upstream_dependencies.push(DatasetDependency::resolved(
                dataset_handle,
                dataset_entry.owner_id,
                dataset_entry.owner_name,
            ));
        }

        Ok(upstream_dependencies)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
