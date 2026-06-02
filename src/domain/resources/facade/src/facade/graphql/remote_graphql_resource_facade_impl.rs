// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::GraphqlHttpClient;
use internal_error::InternalError;
use kamu_resources as domain;
use url::Url;

use crate::facade::graphql::{cynic_api, outcome_mapper};
use crate::{
    ApplyManifestError,
    ApplyManifestRequest,
    BatchResourceError,
    BatchResourceResponse,
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceFacade for RemoteGraphqlResourceFacadeImpl {
    async fn list_supported_kinds(
        &self,
    ) -> Result<Vec<domain::ResourceKindDescriptor>, ListSupportedResourceKindsError> {
        use cynic_api::operations::supported_kinds as Operation;

        let response: Operation::SupportedKindsQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation())
            .await?;

        Ok(response
            .resources
            .supported_kinds
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<domain::ResourcesSummary, ResourcesSummaryError> {
        use cynic_api::operations::summary as Operation;

        let variables =
            Operation::SummaryVariables::new(&request).map_err(ResourcesSummaryError::Internal)?;

        let response: Operation::SummaryQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
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
        use cynic_api::operations::get_resource as Operation;

        let variables = Operation::ResourceSelectorVariables::new(&selector, spec_view_mode)?;

        let response: Operation::GetResourceQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        let Some(resource) = response.resources.resource else {
            return Err(outcome_mapper::not_found_error(&selector));
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

        use cynic_api::operations::get_resources as Operation;

        let variables = Operation::ResourceBatchSelectorVariables::new(&selector, spec_view_mode)?;

        let response: Operation::GetResourcesQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;
        let batch_result = response.resources.resources;

        let successes = outcome_mapper::collect_batch_successes(
            selector.resource_refs.len(),
            batch_result.resources,
            "resource",
            |s| {
                Ok((
                    s.request_index,
                    s.resource
                        .try_into()
                        .map_err(BatchResourceError::Internal)?,
                ))
            },
        )?;

        let problems =
            outcome_mapper::collect_batch_problems(&selector, batch_result.problems, "resource")?;

        outcome_mapper::validate_batch_response_indexes(
            &successes,
            &problems,
            selector.resource_refs.len(),
            "resource",
        )?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn get_identity(
        &self,
        selector: ResourceSelector,
    ) -> Result<domain::ResourceIdentityView, GetResourceError> {
        use cynic_api::operations::identity as Operation;

        let variables = Operation::ResourceIdentitySelectorVariables::new(&selector)?;

        let response: Operation::GetResourceIdentityQuery = self
            .graphql_client
            .execute_operation(Operation::build_identity_operation(variables))
            .await?;

        let Some(identity) = response.resources.resource_identity else {
            return Err(outcome_mapper::not_found_error(&selector));
        };

        Ok(identity.into())
    }

    async fn get_identities(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<domain::ResourceIdentityView, ResourceLookupProblem>,
        BatchResourceError,
    > {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        use cynic_api::operations::identity as Operation;

        let variables = Operation::ResourceIdentityBatchSelectorVariables::new(&selector)?;

        let response: Operation::GetResourceIdentitiesQuery = self
            .graphql_client
            .execute_operation(Operation::build_identities_operation(variables))
            .await?;
        let batch_result = response.resources.resource_identities;

        let successes = outcome_mapper::collect_batch_successes(
            selector.resource_refs.len(),
            batch_result.identities,
            "identity",
            |s| Ok((s.request_index, s.identity.into())),
        )?;

        let problems =
            outcome_mapper::collect_batch_problems(&selector, batch_result.problems, "identity")?;

        outcome_mapper::validate_batch_response_indexes(
            &successes,
            &problems,
            selector.resource_refs.len(),
            "identity",
        )?;

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
        use cynic_api::operations::render_manifest as Operation;

        let variables =
            Operation::RenderResourceManifestVariables::new(&selector, format, spec_view_mode)
                .map_err(RenderResourceManifestError::Internal)?;

        let response: Operation::RenderManifestQuery = self
            .graphql_client
            .execute_operation(Operation::build_manifest_operation(variables))
            .await
            .map_err(RenderResourceManifestError::RemoteRequest)?;

        outcome_mapper::map_render_manifest_outcome(response.resources.render_manifest)
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

        use cynic_api::operations::render_manifest as Operation;

        let variables =
            Operation::RenderResourceManifestsVariables::new(&selector, format, spec_view_mode)
                .map_err(BatchResourceError::Internal)?;

        let response: Operation::RenderManifestsQuery = self
            .graphql_client
            .execute_operation(Operation::build_manifests_operation(variables))
            .await?;
        let batch_result = response.resources.render_manifests;

        let successes = outcome_mapper::collect_batch_successes(
            selector.resource_refs.len(),
            batch_result.manifests,
            "manifest",
            |s| Ok((s.request_index, s.manifest.into())),
        )?;

        let problems =
            outcome_mapper::collect_batch_problems(&selector, batch_result.problems, "manifest")?;

        outcome_mapper::validate_batch_response_indexes(
            &successes,
            &problems,
            selector.resource_refs.len(),
            "manifest",
        )?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListByKindVariables::new(
            &request.kind,
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: Operation::ListByKindQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_by_kind_operation(variables))
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
    ) -> Result<Vec<domain::ResourceIdentityView>, ListResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListByKindVariables::new(
            &request.kind,
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: Operation::ListIdentitiesByKindQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_identities_by_kind_operation(
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
        use cynic_api::operations::search as SearchOperation;

        let variables = SearchOperation::SearchIdentitiesVariables::new(&request)
            .map_err(ListResourcesError::Internal)?;

        let response: SearchOperation::SearchIdentitiesQuery = self
            .graphql_client
            .execute_operation(SearchOperation::build_operation(variables))
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
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListAllVariables::new(
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListAllResourcesError::Internal)?;

        let response: Operation::ListAllQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_all_operation(variables))
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
    ) -> Result<Vec<domain::ResourceIdentityView>, ListAllResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListAllVariables::new(
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListAllResourcesError::Internal)?;

        let response: Operation::ListAllIdentitiesQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_all_identities_operation(variables))
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
        use cynic_api::operations::apply as Operation;

        let variables = Operation::ApplyManifestVariables::new(request, true);

        let response: Operation::ApplyManifestMutation = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        response
            .resources
            .apply_manifest
            .try_into_planning_decision()
            .map_err(Into::into)
    }

    async fn apply_manifest(
        &self,
        request: ApplyManifestRequest,
    ) -> Result<domain::ApplyManifestApplicationDecision, ApplyManifestError> {
        use cynic_api::operations::apply as Operation;

        let variables = Operation::ApplyManifestVariables::new(request, false);

        let response: Operation::ApplyManifestMutation = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        response
            .resources
            .apply_manifest
            .try_into_application_decision()
            .map_err(Into::into)
    }

    async fn delete(
        &self,
        selector: ResourceSelector,
    ) -> Result<domain::ResourceUID, DeleteResourceError> {
        use cynic_api::operations::delete as Operation;

        let variables = Operation::DeleteVariables {
            selector: (&selector)
                .try_into()
                .map_err(DeleteResourceError::Internal)?,
        };

        let response: Operation::DeleteMutation = self
            .graphql_client
            .execute_operation(Operation::build_delete_operation(variables))
            .await
            .map_err(DeleteResourceError::RemoteRequest)?;

        outcome_mapper::map_delete_outcome(response.resources.delete)
    }

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<domain::ResourceUID, ResourceLookupProblem>, BatchResourceError>
    {
        if selector.resource_refs.is_empty() {
            return Ok(BatchResourceResponse::empty());
        }

        use cynic_api::operations::delete as Operation;

        let variables = Operation::DeleteManyVariables {
            selector: (&selector)
                .try_into()
                .map_err(BatchResourceError::Internal)?,
        };

        let response: Operation::DeleteManyMutation = self
            .graphql_client
            .execute_operation(Operation::build_delete_many_operation(variables))
            .await?;
        let batch_result = response.resources.delete_many;

        let successes = outcome_mapper::collect_batch_successes(
            selector.resource_refs.len(),
            batch_result.resources,
            "delete",
            |s| Ok((s.request_index, s.resource_id)),
        )?;

        let problems =
            outcome_mapper::collect_batch_problems(&selector, batch_result.problems, "delete")?;

        outcome_mapper::validate_batch_response_indexes(
            &successes,
            &problems,
            selector.resource_refs.len(),
            "delete",
        )?;

        Ok(BatchResourceResponse {
            successes,
            problems,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
