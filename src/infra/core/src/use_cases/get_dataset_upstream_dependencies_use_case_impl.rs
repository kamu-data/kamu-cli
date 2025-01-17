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
use internal_error::ResultIntoInternal;
use kamu_accounts::AccountService;
use kamu_core::auth::{ClassifyByAllowanceIdsResponse, DatasetAction, DatasetActionAuthorizer};
use kamu_core::{
    DatasetDependency,
    DependencyGraphService,
    GetDatasetUpstreamDependenciesError,
    GetDatasetUpstreamDependenciesUseCase,
    TenancyConfig,
};
use kamu_datasets::DatasetEntryRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn GetDatasetUpstreamDependenciesUseCase)]
pub struct GetDatasetUpstreamDependenciesUseCaseImpl {
    tenancy_config: Arc<TenancyConfig>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    account_service: Arc<dyn AccountService>,
}

impl GetDatasetUpstreamDependenciesUseCaseImpl {
    pub fn new(
        tenancy_config: Arc<TenancyConfig>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
        account_service: Arc<dyn AccountService>,
    ) -> Self {
        Self {
            tenancy_config,
            dependency_graph_service,
            dataset_action_authorizer,
            dataset_entry_repository,
            account_service,
        }
    }
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
            .int_err()?
            .collect::<Vec<_>>()
            .await;

        let mut upstream_dependencies = Vec::with_capacity(upstream_dependency_ids.len());

        let ClassifyByAllowanceIdsResponse {
            authorized_ids,
            unauthorized_ids_with_errors,
        } = self
            .dataset_action_authorizer
            .classify_dataset_ids_by_allowance(upstream_dependency_ids, DatasetAction::Read)
            .await?;

        upstream_dependencies.extend(unauthorized_ids_with_errors.into_iter().map(
            |(unauthorized_dataset_id, _)| DatasetDependency::Unresolved(unauthorized_dataset_id),
        ));

        let dataset_entries_resolution = self
            .dataset_entry_repository
            .get_multiple_dataset_entries(&authorized_ids)
            .await
            .int_err()?;

        let owner_ids = dataset_entries_resolution.resolved_entries_owner_ids();

        upstream_dependencies.extend(
            dataset_entries_resolution
                .unresolved_entries
                .into_iter()
                .map(DatasetDependency::Unresolved),
        );

        let account_map = self
            .account_service
            .get_account_map(owner_ids.into_iter().collect())
            .await
            .int_err()?;

        for dataset_entry in dataset_entries_resolution.resolved_entries {
            let maybe_account = account_map.get(&dataset_entry.owner_id);
            if let Some(account) = maybe_account {
                let dataset_alias = self
                    .tenancy_config
                    .make_alias(account.account_name.clone(), dataset_entry.name);
                let dataset_handle = odf::DatasetHandle::new(dataset_entry.id, dataset_alias);

                upstream_dependencies.push(DatasetDependency::resolved(
                    dataset_handle,
                    account.id.clone(),
                    account.account_name.clone(),
                ));
            } else {
                tracing::warn!(
                    "Upstream owner's account not found for dataset: {:?}",
                    &dataset_entry
                );
                upstream_dependencies.push(DatasetDependency::Unresolved(dataset_entry.id));
            }
        }

        Ok(upstream_dependencies)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
