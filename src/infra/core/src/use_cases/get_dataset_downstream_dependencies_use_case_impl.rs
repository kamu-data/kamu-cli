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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{
    DatasetDependency,
    DependencyGraphService,
    GetDatasetDownstreamDependenciesError,
    GetDatasetDownstreamDependenciesUseCase,
    TenancyConfig,
};
use kamu_datasets::DatasetEntryService;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn GetDatasetDownstreamDependenciesUseCase)]
pub struct GetDatasetDownstreamDependenciesUseCaseImpl {
    tenancy_config: Arc<TenancyConfig>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    account_service: Arc<dyn AccountService>,
}

impl GetDatasetDownstreamDependenciesUseCaseImpl {
    pub fn new(
        tenancy_config: Arc<TenancyConfig>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        dataset_entry_service: Arc<dyn DatasetEntryService>,
        account_service: Arc<dyn AccountService>,
    ) -> Self {
        Self {
            tenancy_config,
            dependency_graph_service,
            dataset_action_authorizer,
            dataset_entry_service,
            account_service,
        }
    }
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
            .int_err()?
            .collect::<Vec<_>>()
            .await;

        let mut downstream_dependencies = Vec::with_capacity(downstream_dependency_ids.len());

        // Cut off datasets that we don't have access to
        let authorized_ids = self
            .dataset_action_authorizer
            .classify_dataset_ids_by_allowance(downstream_dependency_ids, DatasetAction::Read)
            .await?
            .authorized_ids;

        let dataset_entries_resolution = self
            .dataset_entry_service
            .get_multiple_entries(&authorized_ids)
            .await
            .int_err()?;

        let owner_ids = dataset_entries_resolution.resolved_entries_owner_ids();

        downstream_dependencies.extend(
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

                downstream_dependencies.push(DatasetDependency::resolved(
                    dataset_handle,
                    account.id.clone(),
                    account.account_name.clone(),
                ));
            } else {
                tracing::warn!(
                    "Downstream owner's account not found for dataset: {:?}",
                    &dataset_entry
                );
                downstream_dependencies.push(DatasetDependency::Unresolved(dataset_entry.id));
            }
        }

        Ok(downstream_dependencies)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
