// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::CurrentAccountSubject;
use kamu_configuration::{
    DatasetSecretSetBindingRepository,
    DatasetVariableSetBindingRepository,
    SecretSetResource,
    VariableSetResource,
};
use kamu_resources::GenericResourceQueryService;

use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetConfigurationMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetConfigurationMut<'a> {
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
        name = DatasetConfigurationMut_replace_variable_set_bindings,
        skip_all
    )]
    async fn replace_variable_set_bindings(
        &self,
        ctx: &Context<'_>,
        resource_ids: Vec<ResourceID<'static>>,
    ) -> Result<ReplaceDatasetBindingsResult> {
        let dataset_id = self.dataset_request_state.dataset_id();

        let ids: Vec<kamu_resources::ResourceID> =
            resource_ids.into_iter().map(Into::into).collect();

        if let Some(duplicate_id) = find_first_duplicate(&ids) {
            return Ok(ReplaceDatasetBindingsResult::DuplicateResources(
                ReplaceBindingsDuplicateResources {
                    resource_id: duplicate_id.into(),
                },
            ));
        }

        let (current_account_subject, variable_set_binding_repo, generic_resource_query_svc) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn DatasetVariableSetBindingRepository,
            dyn GenericResourceQueryService
        );

        let current_account_id = current_account_subject.account_id().clone();

        let outcome = generic_resource_query_svc
            .find_owned_snapshots(&current_account_id, VariableSetResource::SCHEMA, &ids)
            .await
            .int_err()?;

        if let Some(&id) = outcome.access_denied.first() {
            return Ok(ReplaceDatasetBindingsResult::ResourceAccountMismatch(
                ReplaceBindingsResourceAccountMismatch {
                    resource_id: id.into(),
                },
            ));
        }

        if let Some(&id) = outcome.not_found.first() {
            return Ok(ReplaceDatasetBindingsResult::ResourceNotFound(
                ReplaceBindingsResourceNotFound {
                    resource_id: id.into(),
                },
            ));
        }

        if let Some((id, actual_schema)) = outcome.schema_mismatch.into_iter().next() {
            return Ok(ReplaceDatasetBindingsResult::ResourceSchemaMismatch(
                ReplaceBindingsResourceSchemaMismatch {
                    resource_id: id.into(),
                    expected_schema: VariableSetResource::SCHEMA.to_string(),
                    actual_schema,
                },
            ));
        }

        variable_set_binding_repo
            .replace_bindings(dataset_id, &ids)
            .await
            .int_err()?;

        Ok(ReplaceDatasetBindingsResult::Success(
            ReplaceDatasetBindingsSuccess,
        ))
    }

    #[tracing::instrument(
        level = "info",
        name = DatasetConfigurationMut_replace_secret_set_bindings,
        skip_all
    )]
    async fn replace_secret_set_bindings(
        &self,
        ctx: &Context<'_>,
        resource_ids: Vec<ResourceID<'static>>,
    ) -> Result<ReplaceDatasetBindingsResult> {
        let dataset_id = self.dataset_request_state.dataset_id();

        let ids: Vec<kamu_resources::ResourceID> =
            resource_ids.into_iter().map(Into::into).collect();

        if let Some(duplicate_id) = find_first_duplicate(&ids) {
            return Ok(ReplaceDatasetBindingsResult::DuplicateResources(
                ReplaceBindingsDuplicateResources {
                    resource_id: duplicate_id.into(),
                },
            ));
        }

        let (current_account_subject, secret_set_binding_repo, generic_resource_query_svc) = from_catalog_n!(
            ctx,
            CurrentAccountSubject,
            dyn DatasetSecretSetBindingRepository,
            dyn GenericResourceQueryService
        );

        let current_account_id = current_account_subject.account_id().clone();

        let outcome = generic_resource_query_svc
            .find_owned_snapshots(&current_account_id, SecretSetResource::SCHEMA, &ids)
            .await
            .int_err()?;

        if let Some(&id) = outcome.access_denied.first() {
            return Ok(ReplaceDatasetBindingsResult::ResourceAccountMismatch(
                ReplaceBindingsResourceAccountMismatch {
                    resource_id: id.into(),
                },
            ));
        }

        if let Some(&id) = outcome.not_found.first() {
            return Ok(ReplaceDatasetBindingsResult::ResourceNotFound(
                ReplaceBindingsResourceNotFound {
                    resource_id: id.into(),
                },
            ));
        }

        if let Some((id, actual_schema)) = outcome.schema_mismatch.into_iter().next() {
            return Ok(ReplaceDatasetBindingsResult::ResourceSchemaMismatch(
                ReplaceBindingsResourceSchemaMismatch {
                    resource_id: id.into(),
                    expected_schema: SecretSetResource::SCHEMA.to_string(),
                    actual_schema,
                },
            ));
        }

        secret_set_binding_repo
            .replace_bindings(dataset_id, &ids)
            .await
            .int_err()?;

        Ok(ReplaceDatasetBindingsResult::Success(
            ReplaceDatasetBindingsSuccess,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn find_first_duplicate(ids: &[kamu_resources::ResourceID]) -> Option<kamu_resources::ResourceID> {
    let mut seen = std::collections::HashSet::new();
    ids.iter().find(|&&id| !seen.insert(id)).copied()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ReplaceDatasetBindingsResult {
    Success(ReplaceDatasetBindingsSuccess),
    ResourceNotFound(ReplaceBindingsResourceNotFound),
    ResourceSchemaMismatch(ReplaceBindingsResourceSchemaMismatch),
    ResourceAccountMismatch(ReplaceBindingsResourceAccountMismatch),
    DuplicateResources(ReplaceBindingsDuplicateResources),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct ReplaceDatasetBindingsSuccess;

#[Object]
impl ReplaceDatasetBindingsSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReplaceBindingsResourceNotFound {
    pub resource_id: ResourceID<'static>,
}

#[ComplexObject]
impl ReplaceBindingsResourceNotFound {
    async fn message(&self) -> String {
        format!("Resource '{}' not found", self.resource_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReplaceBindingsResourceSchemaMismatch {
    pub resource_id: ResourceID<'static>,
    pub expected_schema: String,
    pub actual_schema: String,
}

#[ComplexObject]
impl ReplaceBindingsResourceSchemaMismatch {
    async fn message(&self) -> String {
        format!(
            "Resource '{}' has schema '{}' but expected '{}'",
            self.resource_id, self.actual_schema, self.expected_schema
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReplaceBindingsResourceAccountMismatch {
    pub resource_id: ResourceID<'static>,
}

#[ComplexObject]
impl ReplaceBindingsResourceAccountMismatch {
    async fn message(&self) -> String {
        format!(
            "Resource '{}' does not belong to the current account",
            self.resource_id
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ReplaceBindingsDuplicateResources {
    pub resource_id: ResourceID<'static>,
}

#[ComplexObject]
impl ReplaceBindingsDuplicateResources {
    async fn message(&self) -> String {
        format!(
            "Duplicate resource '{}' in the binding list",
            self.resource_id
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
