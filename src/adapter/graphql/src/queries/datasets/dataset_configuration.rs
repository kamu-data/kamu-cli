// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::CurrentAccountSubject;
use kamu_configuration::{DatasetSecretSetBindingRepository, DatasetVariableSetBindingRepository};
use kamu_resources::GenericResourceQueryService;

use crate::prelude::*;
use crate::queries::{DatasetRequestState, ResourceKind};
use crate::scalars::ResourceID;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetConfiguration<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetConfiguration<'a> {
    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::check_dataset_own_access(ctx, dataset_request_state).await?;
        Ok(Self {
            dataset_request_state,
        })
    }

    #[tracing::instrument(
        level = "info",
        name = DatasetConfiguration_variable_set_bindings,
        skip_all
    )]
    async fn variable_set_bindings(&self, ctx: &Context<'_>) -> Result<Vec<DatasetBindingView>> {
        let dataset_id = self.dataset_request_state.dataset_id();
        let (current_account_subject, variable_set_binding_repo, generic_resource_query_svc) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn DatasetVariableSetBindingRepository,
            dyn GenericResourceQueryService
        );

        let bindings = variable_set_binding_repo
            .list_bindings(dataset_id)
            .await
            .int_err()?;

        if bindings.is_empty() {
            return Ok(vec![]);
        }

        let current_account_id = current_account_subject.account_id();

        let uids: Vec<_> = bindings.iter().map(|b| b.resource_uid).collect();
        let identities = generic_resource_query_svc
            .find_resource_identities_by_uids(current_account_id, &uids)
            .await
            .int_err()?;

        let identity_map: std::collections::HashMap<_, _> =
            identities.into_iter().map(|i| (i.uid, i)).collect();

        Ok(bindings
            .into_iter()
            .filter_map(|b| {
                identity_map
                    .get(b.resource_uid.as_ref())
                    .map(|identity| DatasetBindingView {
                        resource_id: kamu_resources::ResourceUID::new(identity.uid).into(),
                        binding_order: b.binding_order,
                        resource_name: identity.name.clone(),
                        resource_kind: ResourceKind::new(identity.kind.clone()),
                        api_version: identity.api_version.clone(),
                    })
            })
            .collect())
    }

    #[tracing::instrument(
        level = "info",
        name = DatasetConfiguration_secret_set_bindings,
        skip_all
    )]
    async fn secret_set_bindings(&self, ctx: &Context<'_>) -> Result<Vec<DatasetBindingView>> {
        let dataset_id = self.dataset_request_state.dataset_id();
        let (current_account_subject, secret_set_binding_repo, generic_resource_query_svc) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn DatasetSecretSetBindingRepository,
            dyn GenericResourceQueryService
        );

        let bindings = secret_set_binding_repo
            .list_bindings(dataset_id)
            .await
            .int_err()?;

        if bindings.is_empty() {
            return Ok(vec![]);
        }

        let current_account_id = current_account_subject.account_id();

        let uids: Vec<_> = bindings.iter().map(|b| b.resource_uid).collect();
        let identities = generic_resource_query_svc
            .find_resource_identities_by_uids(current_account_id, &uids)
            .await
            .int_err()?;

        let identity_map: std::collections::HashMap<_, _> =
            identities.into_iter().map(|i| (i.uid, i)).collect();

        Ok(bindings
            .into_iter()
            .filter_map(|b| {
                identity_map
                    .get(b.resource_uid.as_ref())
                    .map(|identity| DatasetBindingView {
                        resource_id: kamu_resources::ResourceUID::new(identity.uid).into(),
                        binding_order: b.binding_order,
                        resource_name: identity.name.clone(),
                        resource_kind: ResourceKind::new(identity.kind.clone()),
                        api_version: identity.api_version.clone(),
                    })
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct DatasetBindingView {
    pub resource_id: ResourceID,
    pub binding_order: u64,
    pub resource_name: String,
    pub resource_kind: ResourceKind,
    pub api_version: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
