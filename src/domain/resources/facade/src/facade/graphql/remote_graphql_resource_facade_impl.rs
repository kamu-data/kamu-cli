// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::{GraphqlHttpClient, GraphqlHttpRequestError};
use internal_error::InternalError;
use kamu_resources as domain;
use kamu_resources::{ResourceIdentityView, ResourceKindDescriptor, ResourcesSummary};
use url::Url;

use super::{cynic_api, error_mapper, fragments, query_builder};
use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    BatchResourceResponse,
    BatchResourceSuccess,
    DeleteResourceError,
    GetResourceError,
    ListAllResourceIdentitiesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceIdentitiesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceKindsError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceSelector,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourceIdentitiesRequest,
    SearchResourceIdentitiesResponse,
    SpecViewMode,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Note: intentionally not a dill component, used via factories
pub struct RemoteGraphqlResourceFacadeImpl {
    graphql_client: GraphqlHttpClient,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteGraphqlResourceFacadeImpl {
    pub fn new(backend_url: &Url, maybe_access_token: Option<String>) -> Self {
        Self {
            graphql_client: GraphqlHttpClient::from_backend_url(backend_url, maybe_access_token),
        }
    }

    async fn execute_graphql<T>(&self, query: &str) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        self.graphql_client.execute(query).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        let response: cynic_api::supported_kinds::SupportedKindsQuery = self
            .graphql_client
            .execute_operation(cynic_api::supported_kinds::build_operation())
            .await?;

        response
            .resources
            .supported_kinds
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(Into::into)
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<ResourcesSummary, ResourcesSummaryError> {
        let variables = cynic_api::summary::SummaryVariables {
            account: request
                .account
                .as_ref()
                .map(TryInto::try_into)
                .transpose()
                .map_err(ResourcesSummaryError::Internal)?,
        };

        let response: cynic_api::summary::SummaryQuery = self
            .graphql_client
            .execute_operation(cynic_api::summary::build_operation(variables))
            .await?;

        response
            .resources
            .summary
            .try_into()
            .map_err(ResourcesSummaryError::Internal)
    }

    async fn get(
        &self,
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<domain::ResourceView, GetResourceError> {
        let variables =
            cynic_api::variables::ResourceSelectorVariables::new(&selector, spec_view_mode)?;

        let response: cynic_api::get_resource::GetResourceQuery = self
            .graphql_client
            .execute_operation(cynic_api::get_resource::build_operation(variables))
            .await?;

        let Some(resource) = response.resources.resource else {
            return Err(error_mapper::not_found_error(&selector));
        };

        resource.try_into().map_err(GetResourceError::Internal)
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<domain::ResourceView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let variables =
            cynic_api::variables::ResourceBatchSelectorVariables::new(&selector, spec_view_mode)?;

        let response: cynic_api::get_resources::GetResourcesQuery = self
            .graphql_client
            .execute_operation(cynic_api::get_resources::build_operation(variables))
            .await?;
        let batch_result = response.resources.resources;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| {
                Ok(BatchResourceSuccess {
                    request_index: usize::try_from(success.request_index).map_err(|_| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote resource success index {} cannot be converted to usize",
                            success.request_index
                        )))
                    })?,
                    item: success
                        .resource
                        .try_into()
                        .map_err(BatchResourceError::Internal)?,
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "resource")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_identity(
        &self,
        selector: ResourceSelector,
    ) -> Result<ResourceIdentityView, GetResourceError> {
        let variables = cynic_api::variables::ResourceIdentitySelectorVariables::new(&selector)?;

        let response: cynic_api::identity::GetResourceIdentityQuery = self
            .graphql_client
            .execute_operation(cynic_api::identity::build_identity_operation(variables))
            .await?;

        let Some(identity) = response.resources.resource_identity else {
            return Err(error_mapper::not_found_error(&selector));
        };

        Ok(identity.into())
    }

    async fn get_identities(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<ResourceIdentityView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let variables =
            cynic_api::variables::ResourceIdentityBatchSelectorVariables::new(&selector)?;

        let response: cynic_api::identity::GetResourceIdentitiesQuery = self
            .graphql_client
            .execute_operation(cynic_api::identity::build_identities_operation(variables))
            .await?;
        let batch_result = response.resources.resource_identities;

        let successes = batch_result
            .identities
            .into_iter()
            .map(|success| {
                Ok(BatchResourceSuccess {
                    request_index: usize::try_from(success.request_index).map_err(|_| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote identity success index {} cannot be converted to usize",
                            success.request_index
                        )))
                    })?,
                    item: success.identity.into(),
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "identity")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn render_manifest(
        &self,
        selector: ResourceSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<RenderResourceManifestResult, RenderResourceManifestError> {
        let variables = cynic_api::variables::RenderResourceManifestVariables::new(
            &selector,
            format,
            spec_view_mode,
        )
        .map_err(RenderResourceManifestError::Internal)?;

        let response: cynic_api::render_manifest::RenderManifestQuery = self
            .graphql_client
            .execute_operation(cynic_api::render_manifest::build_manifest_operation(
                variables,
            ))
            .await
            .map_err(|error| error_mapper::map_render_manifest_remote_error(&selector, error))?;

        let rendered = response.resources.render_manifest;

        Ok(rendered.into())
    }

    async fn render_manifests(
        &self,
        selector: ResourceBatchSelector,
        format: ResourceManifestFormat,
        spec_view_mode: SpecViewMode,
    ) -> Result<
        BatchResourceResponse<RenderResourceManifestResult, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let variables = cynic_api::variables::RenderResourceManifestsVariables::new(
            &selector,
            format,
            spec_view_mode,
        )
        .map_err(BatchResourceError::Internal)?;

        let response: cynic_api::render_manifest::RenderManifestsQuery = self
            .graphql_client
            .execute_operation(cynic_api::render_manifest::build_manifests_operation(
                variables,
            ))
            .await?;
        let batch_result = response.resources.render_manifests;

        let successes = batch_result
            .manifests
            .into_iter()
            .map(|success| {
                Ok(BatchResourceSuccess {
                    request_index: usize::try_from(success.request_index).map_err(|_| {
                        BatchResourceError::Internal(InternalError::new(format!(
                            "Remote manifest success index {} cannot be converted to usize",
                            success.request_index
                        )))
                    })?,
                    item: success.manifest.into(),
                })
            })
            .collect::<Result<Vec<_>, BatchResourceError>>()?;

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "manifest")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListResourcesError> {
        let variables = cynic_api::variables::ListByKindVariables::new(
            &request.kind,
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: cynic_api::list::ListByKindQuery = self
            .graphql_client
            .execute_operation(cynic_api::list::build_list_by_kind_operation(variables))
            .await?;

        response
            .resources
            .list_by_kind
            .nodes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ListResourcesError::Internal)
    }

    async fn list_identities(
        &self,
        request: ListResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListResourcesError> {
        let variables = cynic_api::variables::ListByKindVariables::new(
            &request.kind,
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: cynic_api::list::ListIdentitiesByKindQuery = self
            .graphql_client
            .execute_operation(cynic_api::list::build_list_identities_by_kind_operation(
                variables,
            ))
            .await?;

        Ok(response
            .resources
            .list_identities_by_kind
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn search_identities(
        &self,
        request: SearchResourceIdentitiesRequest,
    ) -> Result<SearchResourceIdentitiesResponse, ListResourcesError> {
        let variables = cynic_api::variables::SearchIdentitiesVariables::new(&request)
            .map_err(ListResourcesError::Internal)?;

        let response: cynic_api::search::SearchIdentitiesQuery = self
            .graphql_client
            .execute_operation(cynic_api::search::build_operation(variables))
            .await?;

        let connection = response.resources.search_identities;

        Ok(SearchResourceIdentitiesResponse {
            items: connection.nodes.into_iter().map(Into::into).collect(),
            total_count: usize::try_from(connection.total_count).map_err(|_| {
                ListResourcesError::Internal(InternalError::new(format!(
                    "Remote search total_count {} cannot be converted to usize",
                    connection.total_count
                )))
            })?,
        })
    }

    async fn list_all(
        &self,
        request: ListAllResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListAllResourcesError> {
        let variables = cynic_api::variables::ListAllVariables::new(
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .map_err(ListAllResourcesError::Internal)?;

        let response: cynic_api::list::ListAllQuery = self
            .graphql_client
            .execute_operation(cynic_api::list::build_list_all_operation(variables))
            .await?;

        response
            .resources
            .list_all
            .nodes
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, InternalError>>()
            .map_err(ListAllResourcesError::Internal)
    }

    async fn list_all_identities(
        &self,
        request: ListAllResourceIdentitiesRequest,
    ) -> Result<Vec<ResourceIdentityView>, ListAllResourcesError> {
        let variables = cynic_api::variables::ListAllVariables::new(
            request.account.as_ref(),
            request.pagination.offset,
            request.pagination.limit,
        )
        .map_err(ListAllResourcesError::Internal)?;

        let response: cynic_api::list::ListAllIdentitiesQuery = self
            .graphql_client
            .execute_operation(cynic_api::list::build_list_all_identities_operation(
                variables,
            ))
            .await?;

        Ok(response
            .resources
            .list_all_identities
            .nodes
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn plan_apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestPlanningDecision, ApplyManifestError> {
        let query = query_builder::apply_manifest_query(&request, true)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_planning_decision()
            .map_err(Into::into)
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
        let query = query_builder::apply_manifest_query(&request, false)?;
        let response: fragments::ApplyManifestMutationDataFragment =
            self.execute_graphql(&query).await?;

        response
            .resources
            .apply_manifest
            .into_application_decision()
            .map_err(Into::into)
    }

    async fn delete(
        &self,
        selector: ResourceSelector,
    ) -> Result<domain::ResourceUID, DeleteResourceError> {
        let query = query_builder::delete_resource_query(&selector)?;

        let response: fragments::DeleteMutationDataFragment = self
            .execute_graphql(&query)
            .await
            .map_err(|error| error_mapper::map_delete_remote_error(&selector, error))?;

        Ok(response.resources.delete.resource_id)
    }

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<domain::ResourceUID, ResourceLookupProblem>, BatchResourceError>
    {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        let query = query_builder::delete_resources_query(&selector)?;

        let response: fragments::DeleteManyMutationDataFragment =
            self.execute_graphql(&query).await?;
        let batch_result = response.resources.delete_many;

        let successes = batch_result
            .resources
            .into_iter()
            .map(|success| BatchResourceSuccess {
                request_index: success.request_index,
                item: success.resource_id,
            })
            .collect();

        let problems =
            error_mapper::collect_batch_problems(&selector, batch_result.problems, "delete")?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
