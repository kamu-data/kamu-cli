// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use futures::{StreamExt, TryStreamExt};
use kamu_accounts::AuthenticationService;
use kamu_core::{DatasetRepository, DependencyGraphService, InternalError};
use kamu_flow_system::*;
use opendatafabric::{AccountID, DatasetID};

pub struct FlowPermissionsPluginImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    authentication_svc: Arc<dyn AuthenticationService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

#[component(pub)]
#[interface(dyn FlowPermissionsPlugin)]
#[scope(Singleton)]
impl FlowPermissionsPluginImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        authentication_svc: Arc<dyn AuthenticationService>,
        dependency_graph_service: Arc<dyn DependencyGraphService>,
    ) -> Self {
        Self {
            dataset_repo,
            authentication_svc,
            dependency_graph_service,
        }
    }
}

#[async_trait::async_trait]
impl FlowPermissionsPlugin for FlowPermissionsPluginImpl {
    async fn get_account_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
        account_id_maybe: Option<AccountID>,
    ) -> Result<Vec<DatasetID>, InternalError> {
        let dependent_dataset_ids: Vec<_> = self
            .dependency_graph_service
            .get_downstream_dependencies(dataset_id)
            .await
            .int_err()?
            .collect()
            .await;

        if let Some(account_id) = account_id_maybe {
            let account_name_maybe = self
                .authentication_svc
                .find_account_name_by_id(&account_id)
                .await
                .int_err()?;

            if let Some(account_name) = account_name_maybe {
                let account_dataset_ids: Vec<_> = self
                    .dataset_repo
                    .get_datasets_by_owner(&account_name)
                    .map_ok(|dataset_handle| dataset_handle.id)
                    .try_collect()
                    .await
                    .int_err()?;

                let filtered_datasets: Vec<_> = dependent_dataset_ids
                    .iter()
                    .filter(|dataset_id| account_dataset_ids.contains(dataset_id))
                    .cloned()
                    .collect();
                return Ok(filtered_datasets);
            }
        }
        Ok(dependent_dataset_ids)
    }
}
