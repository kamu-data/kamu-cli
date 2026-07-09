// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use graphql_http::GraphqlHttpClient;
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
    ListAllResourceHandlesRequest,
    ListAllResourcesError,
    ListAllResourcesRequest,
    ListResourceHandlesRequest,
    ListResourcesError,
    ListResourcesRequest,
    ListSupportedResourceTypesError,
    RenderResourceManifestError,
    RenderResourceManifestResult,
    ResourceBatchSelector,
    ResourceFacade,
    ResourceLookupProblem,
    ResourceManifestFormat,
    ResourceSelector,
    ResourcesSummaryError,
    ResourcesSummaryRequest,
    SearchResourceHandlesRequest,
    SearchResourceHandlesResponse,
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
    async fn list_supported_resource_types(
        &self,
    ) -> Result<Vec<domain::ResourceTypeDescriptor>, ListSupportedResourceTypesError> {
        use cynic_api::operations::supported_resource_types as Operation;

        let response: Operation::SupportedResourceTypesQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation())
            .await?;

        Ok(response
            .resources
            .supported_resource_types
            .into_iter()
            .map(Into::into)
            .collect())
    }

    async fn summary(
        &self,
        request: ResourcesSummaryRequest,
    ) -> Result<domain::ResourcesSummary, ResourcesSummaryError> {
        use cynic_api::operations::summary as Operation;

        let variables = Operation::SummaryVariables::new(&request);

        let response: Operation::SummaryQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_summary_outcome(response.resources.summary)
    }

    async fn get(
        &self,
        selector: ResourceSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<domain::Resource, GetResourceError> {
        use cynic_api::operations::get_resource as Operation;

        let variables = Operation::ResourceSelectorVariables::new(&selector, spec_view_mode)?;

        let response: Operation::GetResourceQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_get_resource_outcome(response.resources.resource)
    }

    async fn get_many(
        &self,
        selector: ResourceBatchSelector,
        spec_view_mode: SpecViewMode,
    ) -> Result<BatchResourceResponse<domain::Resource, ResourceLookupProblem>, BatchResourceError>
    {
        use cynic_api::operations::get_resources as Operation;

        let variables = Operation::ResourceBatchSelectorVariables::new(&selector, spec_view_mode)?;

        let response: Operation::GetResourcesQuery = self
            .graphql_client
            .execute_operation(Operation::build_operation(variables))
            .await?;

        outcome_mapper::map_batch_get_resources_outcome(response.resources.resources, &selector)
    }

    async fn get_handle(
        &self,
        selector: ResourceSelector,
    ) -> Result<domain::ResourceHandle, GetResourceError> {
        use cynic_api::operations::handle as Operation;

        let variables = Operation::ResourceHandleSelectorVariables::new(&selector)?;

        let response: Operation::GetResourceHandleQuery = self
            .graphql_client
            .execute_operation(Operation::build_handle_operation(variables))
            .await?;

        outcome_mapper::map_get_handle_outcome(response.resources.resource_handle)
    }

    async fn get_handles(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<
        BatchResourceResponse<domain::ResourceHandle, ResourceLookupProblem>,
        BatchResourceError,
    > {
        use cynic_api::operations::handle as Operation;

        let variables = Operation::ResourceHandleBatchSelectorVariables::new(&selector)?;

        let response: Operation::GetResourceHandlesQuery = self
            .graphql_client
            .execute_operation(Operation::build_handles_operation(variables))
            .await?;

        outcome_mapper::map_batch_get_handles_outcome(
            response.resources.resource_handles,
            &selector,
        )
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
            .await?;

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
        use cynic_api::operations::render_manifest as Operation;

        let variables =
            Operation::RenderResourceManifestsVariables::new(&selector, format, spec_view_mode)
                .map_err(BatchResourceError::Internal)?;

        let response: Operation::RenderManifestsQuery = self
            .graphql_client
            .execute_operation(Operation::build_manifests_operation(variables))
            .await?;

        outcome_mapper::map_batch_render_manifests_outcome(
            response.resources.render_manifests,
            &selector,
        )
    }

    async fn list(
        &self,
        request: ListResourcesRequest,
    ) -> Result<Vec<domain::ResourceSummaryView>, ListResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListByResourceTypeVariables::new(
            &request.raw_type_selector,
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: Operation::ListByResourceTypeQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_by_resource_type_operation(variables))
            .await?;

        outcome_mapper::map_list_outcome(response.resources.list_by_resource_type)
    }

    async fn list_handles(
        &self,
        request: ListResourceHandlesRequest,
    ) -> Result<Vec<domain::ResourceHandle>, ListResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListByResourceTypeVariables::new(
            &request.raw_type_selector,
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListResourcesError::Internal)?;

        let response: Operation::ListHandlesByResourceTypeQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_handles_by_resource_type_operation(
                variables,
            ))
            .await?;

        outcome_mapper::map_list_handles_outcome(response.resources.list_handles_by_resource_type)
    }

    async fn search_handles(
        &self,
        request: SearchResourceHandlesRequest,
    ) -> Result<SearchResourceHandlesResponse, ListResourcesError> {
        use cynic_api::operations::search as SearchOperation;

        if request.exact_names.is_none() && request.name_pattern.is_none() {
            return Err(crate::InvalidResourceSearchQueryError.into());
        }

        let variables = SearchOperation::SearchHandlesVariables::new(&request)
            .map_err(ListResourcesError::Internal)?;

        let response: SearchOperation::SearchHandlesQuery = self
            .graphql_client
            .execute_operation(SearchOperation::build_operation(variables))
            .await?;

        outcome_mapper::map_search_handles_outcome(response.resources.search_handles)
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

        outcome_mapper::map_list_all_outcome(response.resources.list_all)
    }

    async fn list_all_handles(
        &self,
        request: ListAllResourceHandlesRequest,
    ) -> Result<Vec<domain::ResourceHandle>, ListAllResourcesError> {
        use cynic_api::operations::list as Operation;

        let variables = cynic_api::variables::ListAllVariables::new(
            request.account.as_ref(),
            request.pagination,
        )
        .map_err(ListAllResourcesError::Internal)?;

        let response: Operation::ListAllHandlesQuery = self
            .graphql_client
            .execute_operation(Operation::build_list_all_handles_operation(variables))
            .await?;

        outcome_mapper::map_list_all_handles_outcome(response.resources.list_all_handles)
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
    }

    async fn delete(
        &self,
        selector: ResourceSelector,
    ) -> Result<domain::ResourceID, DeleteResourceError> {
        use cynic_api::operations::delete as Operation;

        let variables = Operation::DeleteVariables {
            selector: (&selector)
                .try_into()
                .map_err(DeleteResourceError::Internal)?,
        };

        let response: Operation::DeleteMutation = self
            .graphql_client
            .execute_operation(Operation::build_delete_operation(variables))
            .await?;

        outcome_mapper::map_delete_outcome(response.resources.delete)
    }

    async fn delete_many(
        &self,
        selector: ResourceBatchSelector,
    ) -> Result<BatchResourceResponse<domain::ResourceID, ResourceLookupProblem>, BatchResourceError>
    {
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

        outcome_mapper::map_batch_delete_many_outcome(response.resources.delete_many, &selector)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
