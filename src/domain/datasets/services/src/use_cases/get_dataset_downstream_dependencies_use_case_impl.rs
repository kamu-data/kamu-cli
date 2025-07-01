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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_datasets::{
    DatasetDependency,
    DatasetEntryService,
    DependencyGraphService,
    GetDatasetDownstreamDependenciesError,
    GetDatasetDownstreamDependenciesUseCase,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn GetDatasetDownstreamDependenciesUseCase)]
pub struct GetDatasetDownstreamDependenciesUseCaseImpl {
    tenancy_config: Arc<TenancyConfig>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
}

#[async_trait::async_trait]
impl GetDatasetDownstreamDependenciesUseCase for GetDatasetDownstreamDependenciesUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "GetDatasetDownstreamDependenciesUseCase::execute",
        skip_all,
        fields(dataset_id)
    )]
    async fn execute(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetDependency>, GetDatasetDownstreamDependenciesError> {
        use tokio_stream::StreamExt;

        // TODO: PERF: chunk the stream
        let downstream_dependency_ids = self
            .dependency_graph_service
            .get_downstream_dependencies(dataset_id)
            .await
            .collect::<Vec<_>>()
            .await;
        if downstream_dependency_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut downstream_dependencies = Vec::with_capacity(downstream_dependency_ids.len());

        // NOTE: Borrow-checker can't digest the upfront transformation during stream
        //       data collection. So we need to make an additional vector.
        let downstream_dependency_ids = downstream_dependency_ids
            .into_iter()
            .map(Cow::Owned)
            .collect::<Vec<_>>();
        // Cut off datasets that we don't have access to
        let authorized_ids = self
            .dataset_action_authorizer
            .classify_dataset_ids_by_allowance(&downstream_dependency_ids, DatasetAction::Read)
            .await?
            .authorized_ids
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

            downstream_dependencies.push(DatasetDependency::resolved(
                dataset_handle,
                dataset_entry.owner_id,
                dataset_entry.owner_name,
            ));
        }

        Ok(downstream_dependencies)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
